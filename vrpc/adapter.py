# vrpc/adapter.py

import asyncio
import importlib.util
import inspect
import json
import logging
from functools import partial
from pathlib import Path

from docstring_parser import parse
from nanoid import generate as nanoid
from pyee.asyncio import AsyncIOEventEmitter


class VrpcAdapter:
    """
    A Python implementation mirroring the capabilities of VrpcAdapter.js.
    This class is not meant to be instantiated; it provides a static API.
    """

    # --- Static Member Initialization ---
    _function_registry = {}
    _instances = {}
    _emitter = AsyncIOEventEmitter()
    _callback = None
    _correlation_id = 0
    _listeners = {}
    _must_track_client = False
    # Functions that should not be exposed via RPC
    _blacklist = {
        "__init__",
        "__new__",
        "__str__",
        "__repr__",
        "__dict__",
        "__module__",
        "__weakref__",
        "__doc__",
        "__hash__",
        "__eq__",
        "__lt__",
        "__le__",
        "__gt__",
        "__ge__",
        "__getattribute__",
        "__setattr__",
        "__delattr__",
        "__dir__",
    }

    # --- Public API ---

    @classmethod
    def add_plugin_path(cls, dir_path, max_level=float("inf"), current_level=0):
        if current_level >= max_level:
            return

        p = Path(dir_path)
        if not p.is_dir():
            return

        for file_path in p.iterdir():
            if file_path.is_dir():
                cls.add_plugin_path(str(file_path), max_level, current_level + 1)
            elif file_path.is_file() and file_path.suffix == ".py":
                try:
                    module_name = file_path.stem
                    spec = importlib.util.spec_from_file_location(
                        module_name, file_path
                    )
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                except Exception as e:
                    logging.warning(f"Failed to auto-register from {file_path}: {e}")

    @classmethod
    def register(cls, code, only_public=True, with_new=True, schema=None):
        if isinstance(code, str):
            abs_path = Path(code).resolve()
            if not abs_path.exists():
                abs_path = Path(f"{code}.py").resolve()
            module_name = abs_path.stem
            spec = importlib.util.spec_from_file_location(module_name, abs_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            found_class = getattr(module, module_name, None)
            if inspect.isclass(found_class):
                cls._register_class(found_class, only_public, with_new, schema)
            else:
                for name, obj in inspect.getmembers(module):
                    if inspect.isclass(obj) and obj.__module__ == module_name:
                        cls._register_class(obj, only_public, with_new, schema)
                        break
        elif inspect.isclass(code):
            cls._register_class(code, only_public, with_new, schema)
        else:
            raise TypeError("Registration requires a class or a module path string.")

    @classmethod
    def register_instance(cls, obj, class_name, instance, only_public=True):
        member_functions = cls._extract_member_functions(obj.__class__)
        if only_public:
            member_functions = [m for m in member_functions if not m.startswith("_")]

        if class_name not in cls._function_registry:
            cls._function_registry[class_name] = {
                "Klass": obj.__class__,
                "with_new": True,
                "static_functions": [],
                "member_functions": member_functions,
                "schema": None,
                "meta": {},
            }

        cls._instances[instance] = {
            "instance": obj,
            "class_name": class_name,
            "is_isolated": False,
        }

    @classmethod
    def create(cls, class_name, instance=None, args=None, is_isolated=False):
        instance = instance or nanoid(size=8)
        args = args or []
        json_obj = {"c": class_name, "a": [instance, *args]}

        if is_isolated:
            created_instance = cls._handle_create_isolated(json_obj)
        else:
            created_instance = cls._handle_create_shared(json_obj)

        if "e" in json_obj:
            raise ValueError(json_obj["e"]["message"])
        return created_instance

    @classmethod
    def delete(cls, instance):
        if isinstance(instance, str):
            return cls._delete(instance)
        elif isinstance(instance, object):
            for name, data in list(cls._instances.items()):
                if data["instance"] == instance:
                    return cls._delete(name)
        return False

    @classmethod
    def get_meta_data(cls, class_name):
        entry = cls._function_registry.get(class_name)
        if entry and "meta" in entry:
            return entry["meta"]
        return {}

    @classmethod
    def get_instance(cls, instance_name):
        entry = cls._instances.get(instance_name)
        if entry:
            return entry["instance"]
        raise ValueError(f"Could not find instance: {instance_name}")

    @classmethod
    def get_available_classes(cls):
        return sorted(list(cls._function_registry.keys()))

    @classmethod
    def get_available_instances(cls, class_name):
        return sorted(
            [
                name
                for name, data in cls._instances.items()
                if data["class_name"] == class_name and not data["is_isolated"]
            ]
        )

    @classmethod
    def on_callback(cls, callback):
        cls._callback = callback

    @classmethod
    def call(cls, json_string):
        """
        Public entry point parsing string to object. Returns a string.
        Note: The agent.py uses `_call` directly now to capture `must_track_client`.
        """
        json_obj = json.loads(json_string)
        cls._call(json_obj)
        return json.dumps(json_obj)

    # --- Private Registration & Parsing Implementation ---

    @classmethod
    def _register_class(cls, klass, only_public, with_new, schema):
        class_name = klass.__name__
        static_functions = cls._extract_static_functions(klass)
        if only_public:
            static_functions = [f for f in static_functions if not f.startswith("_")]

        member_functions = cls._extract_member_functions(klass)
        if only_public:
            member_functions = [m for m in member_functions if not m.startswith("_")]

        meta = cls._parse_docstrings(klass)

        cls._function_registry[class_name] = {
            "Klass": klass,
            "with_new": with_new,
            "static_functions": static_functions,
            "member_functions": member_functions,
            "schema": schema,
            "meta": meta,
        }

    @classmethod
    def _parse_docstrings(cls, klass):
        meta = {}

        def get_signature_info(func):
            try:
                sig = inspect.signature(func)
                info = {}
                for param in sig.parameters.values():
                    if param.name == "self":
                        continue
                    info[param.name] = {
                        "default": param.default,
                        "annotation": param.annotation,
                    }
                return info, sig.return_annotation
            except (ValueError, TypeError):
                return {}, inspect.Signature.empty

        def normalize_params(params, sig_info):
            normalized = []
            for p in params:
                info = sig_info.get(p.arg_name, {})
                default_val = info.get("default")
                annotation = info.get("annotation")

                is_optional = default_val is not inspect.Parameter.empty
                type_name = getattr(annotation, "__name__", p.type_name)

                normalized.append(
                    {
                        "name": p.arg_name,
                        "optional": is_optional,
                        "description": p.description,
                        "type": type_name,
                        "default": str(default_val) if is_optional else None,
                    }
                )
            return normalized

        def normalize_return(returns, return_annotation):
            type_name = getattr(return_annotation, "__name__", None)
            if not type_name and returns:
                type_name = returns.type_name
            if not type_name:
                return None
            return {
                "description": returns.description if returns else "",
                "type": type_name,
            }

        constructor_doc = inspect.getdoc(klass.__init__)
        if constructor_doc:
            parsed = parse(constructor_doc)
            sig_info, return_annotation = get_signature_info(klass.__init__)
            params = normalize_params(parsed.params, sig_info)

            params.insert(
                0,
                {
                    "name": "instanceName",
                    "optional": False,
                    "description": "Name of the instance to be created",
                    "type": "string",
                    "default": None,
                },
            )
            meta["__createShared__"] = {
                "description": parsed.short_description or parsed.long_description,
                "params": params,
                "ret": normalize_return(parsed.returns, return_annotation),
            }

        for name, func in inspect.getmembers(klass, predicate=inspect.isfunction):
            if not name.startswith("_") and name != "__init__":
                doc = inspect.getdoc(func)
                if doc:
                    parsed = parse(doc)
                    sig_info, return_annotation = get_signature_info(func)
                    meta[name] = {
                        "description": parsed.short_description
                        or parsed.long_description,
                        "params": normalize_params(parsed.params, sig_info),
                        "ret": normalize_return(parsed.returns, return_annotation),
                    }
        return meta

    # --- Private Execution Handling ---

    @classmethod
    def _call(cls, json_obj):
        """Dispatches a parsed RPC call to the appropriate handler."""
        cls._must_track_client = False
        func = json_obj.get("f")
        handlers = {
            "__createIsolated__": cls._handle_create_isolated,
            "__createShared__": cls._handle_create_shared,
            "__delete__": cls._handle_delete,
            "__callAll__": cls._handle_call_all,
        }
        handler = handlers.get(func, cls._handle_call)
        try:
            handler(json_obj)
        except Exception as e:
            logging.error(f"Error during RPC call: {e}", exc_info=True)
            json_obj["e"] = {"message": str(e)}
        return cls._must_track_client

    @classmethod
    def _handle_create_shared(cls, json_obj):
        try:
            class_name = json_obj["c"]
            instance_id, *args = json_obj["a"]
            instance = cls._create(class_name, instance_id, *args)
            cls._instances[instance_id] = {
                "instance": instance,
                "class_name": class_name,
                "is_isolated": False,
            }
            json_obj["r"] = instance_id
            cls._emitter.emit(
                "create",
                {
                    "className": class_name,
                    "instance": instance_id,
                    "isIsolated": False,
                    "args": args,
                },
            )
            return instance
        except Exception as e:
            json_obj["e"] = {"message": str(e), "cause": None}

    @classmethod
    def _handle_create_isolated(cls, json_obj):
        try:
            class_name = json_obj["c"]
            instance_id, *args = json_obj["a"]
            instance = cls._create(class_name, instance_id, *args)
            cls._instances[instance_id] = {
                "instance": instance,
                "class_name": class_name,
                "is_isolated": True,
            }
            json_obj["r"] = instance_id
            cls._emitter.emit(
                "create",
                {
                    "className": class_name,
                    "instance": instance_id,
                    "isIsolated": True,
                    "args": args,
                },
            )
            return instance
        except Exception as e:
            json_obj["e"] = {"message": str(e), "cause": None}

    @classmethod
    def _handle_delete(cls, json_obj):
        instance_name = json_obj["a"][0]
        cls._emitter.emit(
            "beforeDelete", {"instance": instance_name, "className": json_obj["c"]}
        )
        json_obj["r"] = cls._delete(instance_name)
        cls._emitter.emit(
            "delete", {"instance": instance_name, "className": json_obj["c"]}
        )

    @classmethod
    def _handle_call(cls, json_obj):
        if json_obj["f"] == "removeAllListeners":
            cls._remove_all_listeners(json_obj["a"][0], json_obj["s"], json_obj["c"])
            json_obj["r"] = True
            return

        context_name = json_obj["c"]
        func_name = json_obj["f"]
        args = cls._unwrap_arguments(json_obj, func_name)

        if context_name in cls._function_registry:
            entry = cls._function_registry[context_name]
            target = entry["Klass"]
            func = getattr(target, func_name, None)
            if not (func and callable(func)):
                raise AttributeError(f"Could not find static function: {func_name}")
            result = func(*args)
        elif context_name in cls._instances:
            entry = cls._instances[context_name]
            target = entry["instance"]
            func = getattr(target, func_name, None)
            if not (func and callable(func)):
                raise AttributeError(f"Could not find member function: {func_name}")
            result = func(*args)
        else:
            raise ValueError(f"Could not find context: {context_name}")

        if inspect.isawaitable(result):
            cls._handle_promise(json_obj, result)
        else:
            json_obj["r"] = result

    @classmethod
    def _handle_call_all(cls, json_obj):
        class_name = json_obj["c"]
        func_name = json_obj["a"][0]

        calls = []
        for instance_id, data in cls._instances.items():
            if data["class_name"] != class_name or data["is_isolated"]:
                continue

            try:
                unwrapped = cls._unwrap_arguments(
                    json_obj,
                    func_name=func_name,
                    is_call_all=True,
                    instance_id=instance_id,
                )
                instance = data["instance"]
                func = getattr(instance, func_name, None)

                if not (func and callable(func)):
                    calls.append(
                        {
                            "id": instance_id,
                            "err": f"Could not find function: {func_name}",
                            "val": None,
                        }
                    )
                    continue

                val = func(*unwrapped)
                calls.append({"id": instance_id, "val": val, "err": None})
            except Exception as e:
                calls.append({"id": instance_id, "val": None, "err": str(e)})

        has_awaitables = any(inspect.isawaitable(c["val"]) for c in calls)
        if has_awaitables:
            cls._handle_call_all_promise(json_obj, calls)
        else:
            json_obj["r"] = calls

    @classmethod
    def _create(cls, class_name, instance_id, *args):
        if instance_id in cls._instances:
            return cls._instances[instance_id]["instance"]

        registry_entry = cls._function_registry.get(class_name)
        if not registry_entry:
            raise ValueError(f'"{class_name}" is not a registered class')

        Klass = registry_entry["Klass"]
        return Klass(*args)

    @classmethod
    def _delete(cls, instance_id):
        if instance_id in cls._instances:
            del cls._instances[instance_id]
            # Free any registered listeners associated with this instance
            if instance_id in cls._listeners:
                del cls._listeners[instance_id]
            return True
        return False

    # --- Event and Argument Handling ---

    @classmethod
    def _unwrap_arguments(
        cls, json_obj, func_name=None, is_call_all=False, instance_id=None
    ):
        unwrapped = []
        client_id = json_obj.get("s")
        context = instance_id if is_call_all else json_obj.get("c")

        args = json_obj.get("a", [])[1:] if is_call_all else json_obj.get("a", [])

        for arg in args:
            if isinstance(arg, str):
                if arg.startswith("__f__"):
                    callback = partial(
                        cls._generate_callback,
                        event_id=arg,
                        client_id=client_id,
                        is_call_all=is_call_all,
                        instance_id=context,
                    )
                    unwrapped.append(callback)
                elif arg.startswith("__e__"):
                    if func_name in ("on", "addListener"):
                        cls._must_track_client = True
                        event_name = args[0] if len(args) > 0 else None
                        listener = cls._register_listener(
                            client_id, context, arg, event_name, is_call_all
                        )
                        unwrapped.append(listener)
                    elif func_name in ("off", "removeListener"):
                        listener = cls._unregister_listener(client_id, context, arg)
                        unwrapped.append(listener)
                    else:
                        cls._must_track_client = True
                        listener = cls._register_listener(
                            client_id, context, arg, None, is_call_all
                        )
                        unwrapped.append(listener)
                else:
                    unwrapped.append(arg)
            else:
                unwrapped.append(arg)
        return unwrapped

    @classmethod
    def _generate_callback(
        cls, *inner_args, event_id, client_id, is_call_all=False, instance_id=None
    ):
        if not cls._callback:
            logging.warning("VrpcAdapter has no callback handler set.")
            return

        a = [instance_id, *inner_args] if is_call_all else list(inner_args)
        cls._callback({"a": a, "s": client_id, "i": event_id})

    @classmethod
    def _register_listener(
        cls, client_id, instance_id, event_id, event_name, is_call_all
    ):
        if instance_id not in cls._listeners:
            cls._listeners[instance_id] = {}

        if event_id not in cls._listeners[instance_id]:
            listener = partial(
                cls._generate_listener,
                event_id=event_id,
                instance_id=instance_id,
                is_call_all=is_call_all,
            )
            cls._listeners[instance_id][event_id] = {
                "event": event_name,
                "clients": [client_id],
                "listener": listener,
            }
            return listener

        if client_id not in cls._listeners[instance_id][event_id]["clients"]:
            cls._listeners[instance_id][event_id]["clients"].append(client_id)
        return cls._listeners[instance_id][event_id]["listener"]

    @classmethod
    def _generate_listener(cls, *inner_args, event_id, instance_id, is_call_all):
        a = [instance_id, *inner_args] if is_call_all else list(inner_args)
        if cls._callback:
            cls._callback({"a": a, "i": event_id})

    @classmethod
    def _unregister_listener(cls, client_id, instance_id, event_id):
        if instance_id in cls._listeners and event_id in cls._listeners[instance_id]:
            entry = cls._listeners[instance_id][event_id]
            if client_id in entry["clients"]:
                entry["clients"].remove(client_id)
            if not entry["clients"]:
                listener = entry["listener"]
                del cls._listeners[instance_id][event_id]
                if not cls._listeners[instance_id]:
                    del cls._listeners[instance_id]
                return listener
        return None

    @classmethod
    def _remove_all_listeners(cls, event_name, client_id, instance_id):
        if instance_id not in cls._listeners:
            return

        events_to_delete = []
        for event_id, data in cls._listeners[instance_id].items():
            if data["event"] == event_name and client_id in data["clients"]:
                data["clients"] = [c for c in data["clients"] if c != client_id]
                try:
                    instance = cls.get_instance(instance_id)
                    if hasattr(instance, "remove_all_listeners"):
                        instance.remove_all_listeners(event_name)
                except ValueError:
                    pass

                if not data["clients"]:
                    events_to_delete.append(event_id)

        for event_id in events_to_delete:
            del cls._listeners[instance_id][event_id]

        if not cls._listeners[instance_id]:
            del cls._listeners[instance_id]

    @classmethod
    def _unregister_client(cls, client_id):
        """Called by the Agent when a client drops off"""
        for instance_id, events in list(cls._listeners.items()):
            for event_id, data in list(events.items()):
                if client_id in data["clients"]:
                    data["clients"].remove(client_id)
                    if not data["clients"]:
                        try:
                            instance = cls.get_instance(instance_id)
                            if hasattr(instance, "remove_listener"):
                                instance.remove_listener(
                                    data["event"], data["listener"]
                                )
                        except ValueError:
                            pass
                        del events[event_id]
            if not events:
                del cls._listeners[instance_id]

    # --- Promise Handlers ---

    @classmethod
    def _handle_promise(cls, json_obj, awaitable):
        if not cls._callback:
            logging.error("Cannot handle promise, no callback handler is set.")
            return

        cls._correlation_id += 1
        promise_id = f"__p__{json_obj['f']}-{cls._correlation_id}"
        json_obj["r"] = promise_id

        async def await_and_callback():
            try:
                result = await awaitable
                cls._callback({**json_obj, "r": result, "i": promise_id})
            except Exception as e:
                cls._callback({**json_obj, "e": {"message": str(e)}, "i": promise_id})

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(await_and_callback())
        except RuntimeError:
            logging.error("No running asyncio event loop to handle promise.")

    @classmethod
    def _handle_call_all_promise(cls, json_obj, calls):
        if not cls._callback:
            logging.error("Cannot handle promise, no callback handler is set.")
            return

        cls._correlation_id += 1
        promise_id = f"__p__{json_obj['f']}-{cls._correlation_id}"
        json_obj["r"] = promise_id

        async def await_all():
            results = []
            for call in calls:
                if inspect.isawaitable(call["val"]):
                    try:
                        val = await call["val"]
                        results.append({"id": call["id"], "val": val, "err": None})
                    except Exception as e:
                        results.append({"id": call["id"], "val": None, "err": str(e)})
                else:
                    results.append(call)

            cls._callback({**json_obj, "r": results, "i": promise_id})

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(await_all())
        except RuntimeError:
            logging.error("No running asyncio event loop to handle promise.")

    # --- Python Utilities ---

    @classmethod
    def _extract_member_functions(cls, klass):
        return [
            name
            for name, func in inspect.getmembers(klass, predicate=inspect.isfunction)
            if name not in cls._blacklist
        ]

    @classmethod
    def _extract_static_functions(cls, klass):
        return [
            name
            for name, func in inspect.getmembers(klass)
            if (inspect.isfunction(func) or inspect.isbuiltin(func))
            and name in klass.__dict__
        ]
