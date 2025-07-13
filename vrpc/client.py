# vrpc/client.py

import asyncio
import hashlib
import json
import logging
import os
import platform
from collections import defaultdict
from contextlib import suppress

import aiomqtt
from nanoid import generate as nanoid

from .adapter import EventEmitter

logger = logging.getLogger(__name__)

VRPC_PROTOCOL_VERSION = 3


class _VrpcProxy:
    """A proxy object that forwards method calls to a remote instance."""

    def __init__(self, client, agent, class_name, instance):
        self._client = client
        self._agent = agent
        self._class_name = class_name
        self._instance = instance
        self.vrpc_instance_id = instance

    def __getattr__(self, name):
        async def remote_method(*args):
            return await self._client._call_remote_method(
                self._agent, self._instance, name, list(args)
            )

        return remote_method


class VrpcClient(EventEmitter):
    """Client for creating proxies and remotely calling functions."""

    def __init__(
        self,
        domain: str,
        agent: str = "*",
        broker: str = "mqtts://vrpc.io:8883",
        username: str = None,
        password: str = None,
        token: str = None,
        timeout: int = 12,
        identity: str = None,
        best_effort: bool = True,
        requires_schema: bool = False,
    ):
        super().__init__()
        if not domain or any(c in domain for c in "+/#*"):
            raise ValueError(
                "Domain must be specified and cannot contain +, /, #, or *"
            )
        if any(c in agent for c in "+/#"):
            raise ValueError("Agent cannot contain +, /, or #")

        self.domain = domain
        self.default_agent = agent
        self.broker = broker
        self.username = username
        self.password = password
        self.token = token
        self.timeout = timeout
        self.identity = identity
        self.qos = 0 if best_effort else 1
        self.requires_schema = requires_schema

        self._instance = nanoid(size=8)
        self.vrpc_client_id = self._create_vrpc_client_id()
        self.mqtt_client_id = self._create_mqtt_client_id()

        self._agents = defaultdict(lambda: {"classes": {}})
        self._pending_calls = {}
        self._proxies = {}
        self._invoke_id = 0
        self._client = None
        self._background_task = None

    async def connect(self):
        """Connects to the MQTT broker and starts listening for messages."""
        if self._client:
            return

        username = self.username
        password = self.password
        if self.token:
            username = username or f"{self.domain}:client@{platform.node()}-py"
            password = self.token

        will = aiomqtt.Will(
            topic=f"{self.vrpc_client_id}/__clientInfo__",
            payload=json.dumps({"status": "offline"}),
            qos=self.qos,
            retain=True,
        )

        self._client = aiomqtt.Client(
            hostname=self.broker.split("://")[1].split(":")[0],
            port=int(self.broker.split(":")[-1]),
            username=username,
            password=password,
            identifier=self.mqtt_client_id,
            will=will,
            tls_params=aiomqtt.TLSParameters() if "mqtts" in self.broker else None,
        )

        self._background_task = asyncio.create_task(self._message_loop())

    async def _message_loop(self):
        """The main loop for connecting and handling incoming messages."""
        try:
            async with self._client as client:
                await self._handle_connect()
                async for message in client.messages:
                    await self._handle_message(message)
        except aiomqtt.MqttError as error:
            logger.error(f"Client MQTT Error: {error}")
            self.emit("error", error)
        except asyncio.CancelledError:
            logger.info("Client message loop cancelled.")

    async def _handle_connect(self):
        """Subscribes to topics after a successful connection."""
        logger.info("Client connected successfully.")
        agent_filter = self.default_agent if self.default_agent != "*" else "+"

        await self._client.subscribe(f"{self.domain}/{agent_filter}/__agentInfo__")
        info_topic = "__classInfo__" if self.requires_schema else "__classInfoConcise__"
        await self._client.subscribe(f"{self.domain}/{agent_filter}/+/{info_topic}")

        await self._client.subscribe(self.vrpc_client_id)
        self.emit("connect")

    async def _handle_message(self, message: aiomqtt.Message):
        """Handles and routes all incoming MQTT messages."""
        topic = message.topic.value
        logger.debug(f"Client received message on topic: {topic}")
        try:
            payload = json.loads(message.payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning(f"Could not decode message on topic: {topic}")
            return

        if topic == self.vrpc_client_id:
            call_id = payload.get("i")
            if call_id in self._pending_calls:
                future = self._pending_calls.pop(call_id)
                if "e" in payload and payload["e"]:
                    future.set_exception(
                        RuntimeError(
                            payload["e"].get("message", "Unknown remote error")
                        )
                    )
                else:
                    result = payload.get("r")
                    if isinstance(result, str) and result.startswith("__p__"):
                        self._pending_calls[result] = future
                    else:
                        future.set_result(result)
            return

        tokens = topic.split("/")
        if len(tokens) == 3 and tokens[2] == "__agentInfo__":
            domain, agent, _ = tokens
            self._agents[agent].update(payload)
            self.emit("agent", {"domain": domain, "agent": agent, **payload})

        elif len(tokens) == 4 and tokens[3] in (
            "__classInfo__",
            "__classInfoConcise__",
        ):
            domain, agent, class_name, _ = tokens
            old_info = self._agents[agent]["classes"].get(class_name, {})
            self._agents[agent]["classes"][class_name] = payload
            new_instances = set(payload.get("instances", []))
            old_instances = set(old_info.get("instances", []))
            added = list(new_instances - old_instances)
            removed = list(old_instances - new_instances)
            if added:
                self.emit(
                    "instanceNew",
                    added,
                    {"domain": domain, "agent": agent, "className": class_name},
                )
            if removed:
                self.emit(
                    "instanceGone",
                    removed,
                    {"domain": domain, "agent": agent, "className": class_name},
                )
            self.emit("class", {"domain": domain, "agent": agent, **payload})

    def get_client_id(self):
        return self.vrpc_client_id

    async def create(
        self,
        class_name: str,
        instance: str = None,
        args: list = None,
        agent: str = None,
        is_isolated: bool = False,
        cache_proxy: bool = False,
    ):
        instance = instance or nanoid(size=8)
        args = args or []
        agent = agent or self.default_agent
        if agent == "*":
            raise ValueError("An explicit agent must be specified for creation.")

        function_name = "__createIsolated__" if is_isolated else "__createShared__"
        await self._call_static_remote_method(
            agent, class_name, function_name, [instance, *args]
        )

        proxy = _VrpcProxy(self, agent, class_name, instance)
        if cache_proxy:
            self._proxies[instance] = proxy
        return proxy

    async def get_instance(
        self, instance: str, agent: str = None, class_name: str = None
    ):
        if instance in self._proxies:
            return self._proxies[instance]
        agent, class_name = await self._find_instance_info(instance, agent, class_name)
        return _VrpcProxy(self, agent, class_name, instance)

    async def call_static(
        self, class_name: str, function_name: str, args: list = None, agent: str = None
    ):
        args = args or []
        agent = agent or self.default_agent
        if agent == "*":
            raise ValueError("An explicit agent must be specified for static calls.")
        return await self._call_static_remote_method(
            agent, class_name, function_name, args
        )

    async def end(self):
        """Publishes offline status and cancels the background message loop."""
        if self._client:
            try:
                # FIX: Explicitly publish offline status for a graceful shutdown
                await self._client.publish(
                    f"{self.vrpc_client_id}/__clientInfo__",
                    json.dumps({"status": "offline"}),
                    qos=self.qos,
                    retain=True,
                )
            except aiomqtt.MqttError as e:
                logger.warning(f"Could not publish offline status: {e}")

        if self._background_task:
            self._background_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._background_task
        self.emit("end")

    def get_system_information(self) -> dict:
        return dict(self._agents)

    def get_available_agents(self, must_be_online: bool = True) -> list:
        return [
            agent
            for agent, data in self._agents.items()
            if not must_be_online or data.get("status") == "online"
        ]

    def _create_vrpc_client_id(self) -> str:
        identity = self.identity or self._instance
        return f"{self.domain}/{platform.node()}/{identity}"

    def _create_mqtt_client_id(self) -> str:
        client_info = f"{platform.machine()}{os.path.expanduser('~')}{platform.node()}"
        md5 = hashlib.md5(client_info.encode()).hexdigest()[:12]
        return f"vc3{self._instance}{md5}"

    async def _find_instance_info(self, instance, agent=None, class_name=None):
        for _ in range(self.timeout * 10):
            agents_to_search = [agent] if agent else self.get_available_agents()
            for agent_name in agents_to_search:
                classes = self._agents.get(agent_name, {}).get("classes", {})
                for klass, data in classes.items():
                    if class_name and klass != class_name:
                        continue
                    if instance in data.get("instances", []):
                        return agent_name, klass
            await asyncio.sleep(0.1)
        raise ValueError(f"Instance '{instance}' could not be found.")

    async def _call_static_remote_method(self, agent, class_name, function_name, args):
        topic = f"{self.domain}/{agent}/{class_name}/__static__/{function_name}"
        return await self._execute_remote_call(topic, {"a": args})

    async def _call_remote_method(self, agent, instance, function_name, args):
        _, class_name = await self._find_instance_info(instance, agent=agent)
        topic = f"{self.domain}/{agent}/{class_name}/{instance}/{function_name}"
        return await self._execute_remote_call(topic, {"a": args})

    async def _execute_remote_call(self, topic, json_obj):
        self._invoke_id += 1
        call_id = f"{self._instance}-{self.get_client_id()}-{self._invoke_id}"
        payload = {
            **json_obj,
            "i": call_id,
            "s": self.vrpc_client_id,
            "v": VRPC_PROTOCOL_VERSION,
        }
        future = asyncio.get_event_loop().create_future()
        self._pending_calls[call_id] = future
        await self._client.publish(topic, json.dumps(payload), qos=self.qos)
        try:
            return await asyncio.wait_for(future, self.timeout)
        except asyncio.TimeoutError:
            self._pending_calls.pop(call_id, None)
            raise TimeoutError(f"Call to topic {topic} timed out (> {self.timeout}s)")
