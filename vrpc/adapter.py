# vrpc/adapter.py

import importlib.util
import inspect
import json
import logging
from collections import defaultdict
from functools import partial, wraps
from pathlib import Path

from docstring_parser import parse
from nanoid import generate as nanoid


# A simple EventEmitter implementation to match Node.js's functionality
class EventEmitter:
    def __init__(self):
        self._listeners = defaultdict(list)

    def on(self, event_name, listener):
        self._listeners[event_name].append(listener)

    def once(self, event_name, listener):
        @wraps(listener)
        def wrapper(*args, **kwargs):
            self.off(event_name, wrapper)
            return listener(*args, **kwargs)

        self.on(event_name, wrapper)

    def off(self, event_name, listener):
        if event_name in self._listeners:
            self._listeners[event_name] = [
                l for l in self._listeners[event_name] if l != listener
            ]

    def emit(self, event_name, *args, **kwargs):
        if event_name in self._listeners:
            # Make a copy in case listeners are modified during emission
            for listener in self._listeners[event_name][:]:
                listener(*args, **kwargs)

    def remove_all_listeners(self, event_name=None):
        if event_name:
            if event_name in self._listeners:
                del self._listeners[event_name]
        else:
            self._listeners.clear()


class VrpcAdapter:
    """
    A Python implementation mirroring the capabilities of VrpcAdapter.js.
    This class is not meant to be instantiated; it provides a static API.
    """

    # --- Static Member Initialization ---
    _function_registry = {}
    _instances = {}
    _emitter = EventEmitter()
    _callback = None
    _correlation_id = 0
    _listeners = defaultdict(dict)
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
        """
        Recursively searches for and imports .py files to trigger auto-registration.

        :param dir_path: The directory path to start the search from.
        :param max_level: Maximum recursion depth.
        :param current_level: The current depth of the search.
        """
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
        """
        Registers a class, making it remotely callable.

        :param code: A class object or a string path to a Python module.
        :param only_public: If True, ignores methods starting with '_'.
        :param with_new: If True, instances are created like `Klass()`.
        :param schema: A JSON schema for constructor validation (not implemented yet).
        """
        if isinstance(code, str):
            abs_path = Path(code).resolve()
            if not abs_path.exists():
                # Handle cases like "./fixtures/TestClassDoc" -> "./fixtures/TestClassDoc.py"
                abs_path = Path(f"{code}.py").resolve()
            module_name = abs_path.stem
            spec = importlib.util.spec_from_file_location(module_name, abs_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Find a class that has the same name as the file (a common Python convention)
            found_class = getattr(module, module_name, None)
            if inspect.isclass(found_class):
                cls._register_class(found_class, only_public, with_new, schema)
            else:  # Fallback to finding the first class in the file
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
        """
        Registers an existing object instance.

        :param obj: The instance to register.
        :param class_name: The name to use for the class of this instance.
        :param instance: The name for this specific instance.
        :param only_public: If True, ignores methods starting with '_'.
        """
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
        """
        Creates a new instance of a registered class.
        """
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
        """
        Deletes a managed instance.
        """
        if isinstance(instance, str):
            return cls._delete(instance)
        elif isinstance(instance, object):
            for name, data in list(cls._instances.items()):
                if data["instance"] == instance:
                    return cls._delete(name)
        return False

    @classmethod
    def get_meta_data(cls, class_name):
        """Retrieves the parsed docstring metadata for a given class."""
        entry = cls._function_registry.get(class_name)
        if entry and "meta" in entry:
            return entry["meta"]
        return {}

    @classmethod
    def get_instance(cls, instance_name):
        """Retrieves a managed instance by its name."""
        entry = cls._instances.get(instance_name)
        if entry:
            return entry["instance"]
        raise ValueError(f"Could not find instance: {instance_name}")

    @classmethod
    def get_available_classes(cls):
        """Returns a list of all registered class names."""
        return sorted(list(cls._function_registry.keys()))

    @classmethod
    def get_available_instances(cls, class_name):
        """Returns a list of non-isolated instances for a given class."""
        return sorted(
            [
                name
                for name, data in cls._instances.items()
                if data["class_name"] == class_name and not data["is_isolated"]
            ]
        )

    @classmethod
    def on_callback(cls, callback):
        """Sets the global callback function for sending results back."""
        cls._callback = callback

    @classmethod
    def call(cls, json_string):
        """
        Main entry point for handling an RPC call.
        """
        json_obj = json.loads(json_string)
        cls._call(json_obj)
        return json.dumps(json_obj)

    # --- Private Implementation ---

    @classmethod
    def _register_class(cls, klass, only_public, with_new, schema):
        """Internal helper to register a class object."""
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

    # In vrpc/adapter.py, replace this entire method

    @classmethod
    def _parse_docstrings(cls, klass):
        """
        Parses docstrings and combines them with runtime signature inspection
        to generate metadata that matches the JS version's output.
        """
        meta = {}

        def get_signature_info(func):
            """Helper to extract defaults and type annotations from a signature."""
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
            """Converts docstring_parser output to the expected format."""
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

        # --- Parse the constructor ---
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

        # --- Parse member functions ---
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

    @classmethod
    def _call(cls, json_obj):
        """Dispatches a parsed RPC call to the appropriate handler."""
        func = json_obj.get("f")
        handlers = {
            "__createIsolated__": cls._handle_create_isolated,
            "__createShared__": cls._handle_create_shared,
            "__delete__": cls._handle_delete,
        }
        handler = handlers.get(func, cls._handle_call)
        try:
            handler(json_obj)
        except Exception as e:
            logging.error(f"Error during RPC call: {e}", exc_info=True)
            json_obj["e"] = {"message": str(e)}

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
        context_name = json_obj["c"]
        func_name = json_obj["f"]
        args = cls._unwrap_arguments(json_obj)

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
            if instance_id in cls._listeners:
                del cls._listeners[instance_id]
            return True
        return False

    @classmethod
    def _unwrap_arguments(cls, json_obj):
        unwrapped = []
        args = json_obj.get("a", [])
        client_id = json_obj.get("s")

        for arg in args:
            if isinstance(arg, str):
                if arg.startswith("__f__"):
                    callback = partial(
                        cls._generate_callback, event_id=arg, client_id=client_id
                    )
                    unwrapped.append(callback)
                elif arg.startswith("__e__"):
                    pass
                else:
                    unwrapped.append(arg)
            else:
                unwrapped.append(arg)
        return unwrapped

    @classmethod
    def _generate_callback(cls, *inner_args, event_id, client_id):
        if not cls._callback:
            logging.warning("VrpcAdapter has no callback handler set.")
            return

        payload = {"a": inner_args, "s": client_id, "i": event_id}
        cls._callback(payload)

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

        import asyncio

        # This requires an event loop to be running in the execution context
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(await_and_callback())
        except RuntimeError:
            logging.error("No running asyncio event loop to handle promise.")

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
