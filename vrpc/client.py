# vrpc/client.py

import asyncio
import hashlib
import json
import logging
import os
import platform
from collections import defaultdict

from gmqtt import Client as MqttClient
from gmqtt import Message
from nanoid import generate as nanoid

from .adapter import EventEmitter

logger = logging.getLogger(__name__)

VRPC_PROTOCOL_VERSION = 3


class _VrpcProxy:
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

    async def connect(self):
        """Connects to the MQTT broker and starts listening for system info."""
        if self._client and self._client.is_connected:
            return
        username = self.username
        password = self.password
        if self.token:
            username = username or f"{self.domain}:client@{platform.node()}-py"
            password = self.token
        self._client = MqttClient(self.mqtt_client_id)
        self._client.set_auth_credentials(username, password)

        # FIX: Assign a gmqtt.Message object to the will_message attribute
        self._client.will_message = Message(
            topic=f"{self.vrpc_client_id}/__clientInfo__",
            payload=json.dumps({"status": "offline"}),
            qos=self.qos,
            retain=True,
        )

        self._client.on_connect = self._handle_connect
        self._client.on_message = self._handle_message
        self._client.on_disconnect = lambda *args: self.emit("close")
        host, port_str = (
            self.broker.replace("mqtts://", "").replace("mqtt://", "").split(":")
        )
        logger.info(f"Connecting client '{self.vrpc_client_id}' to {self.broker}...")
        try:
            await asyncio.wait_for(
                self._client.connect(
                    host, int(port_str), ssl=self.broker.startswith("mqtts")
                ),
                timeout=self.timeout,
            )
        except asyncio.TimeoutError:
            raise ConnectionError(f"Connection trial timed out (> {self.timeout}s)")

    def _handle_message(self, client, topic, payload, qos, properties):
        """Callback for incoming MQTT messages, now with robust topic parsing."""
        logger.debug(f"Client received message on topic: {topic}")
        tokens = topic.split("/")
        message = json.loads(payload)

        # FIX: Check topic structure before unpacking
        if len(tokens) == 3 and tokens[2] == "__agentInfo__":
            domain, agent, _ = tokens
            self._agents[agent].update(message)
            self.emit("agent", {"domain": domain, "agent": agent, **message})
        elif len(tokens) == 4 and tokens[3] in (
            "__classInfo__",
            "__classInfoConcise__",
        ):
            domain, agent, class_name, _ = tokens
            old_info = self._agents[agent]["classes"].get(class_name, {})
            self._agents[agent]["classes"][class_name] = message
            new_instances = set(message.get("instances", []))
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
            self.emit("class", {"domain": domain, "agent": agent, **message})
        elif tokens[-1] == "callback":
            call_id = message.get("i")
            if call_id in self._pending_calls:
                future = self._pending_calls.pop(call_id)
                if "e" in message:
                    future.set_exception(RuntimeError(message["e"]))
                else:
                    future.set_result(message.get("r"))
        else:
            logger.warning(f"Client received message on unhandled topic: {topic}")

    # The rest of the file is correct and can remain the same...
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
        json_obj = {
            "c": class_name,
            "f": "__createIsolated__" if is_isolated else "__createShared__",
            "a": [instance, *args],
        }
        await self._call_static_remote_method(
            agent, class_name, json_obj["f"], json_obj["a"]
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
        agent, class_name = self._find_instance_info(instance, agent, class_name)
        return _VrpcProxy(self, agent, class_name, instance)

    async def call_static(
        self, class_name: str, function_name: str, args: list = None, agent: str = None
    ):
        args = args or []
        agent = agent or self.default_agent
        return await self._call_static_remote_method(
            agent, class_name, function_name, args
        )

    async def end(self):
        if self._client and self._client.is_connected:
            self._client.publish(
                f"{self.vrpc_client_id}/__clientInfo__",
                json.dumps({"status": "offline", "v": VRPC_PROTOCOL_VERSION}),
            )
            await self._client.disconnect()
        self.emit("end")

    def get_system_information(self) -> dict:
        return dict(self._agents)

    def get_available_agents(self, must_be_online: bool = True) -> list:
        return [
            agent
            for agent, data in self._agents.items()
            if not must_be_online or data.get("status") == "online"
        ]

    def _handle_connect(self, client, flags, rc, properties):
        logger.info("Client connected successfully.")
        agent_filter = self.default_agent if self.default_agent != "*" else "+"
        client.subscribe(f"{self.domain}/{agent_filter}/__agentInfo__")
        info_topic = "__classInfo__" if self.requires_schema else "__classInfoConcise__"
        client.subscribe(f"{self.domain}/{agent_filter}/+/{info_topic}")
        client.subscribe(f"{self.vrpc_client_id}/-/client/callback")
        self.emit("connect")

    def _create_vrpc_client_id(self) -> str:
        identity = self.identity or self._instance
        return f"{self.domain}/{platform.node()}/{identity}"

    def _create_mqtt_client_id(self) -> str:
        client_info = f"{platform.machine()}{os.path.expanduser('~')}{platform.node()}"
        md5 = hashlib.md5(client_info.encode()).hexdigest()[:12]
        return f"vc3{self._instance}{md5}"

    def _find_instance_info(self, instance, agent=None, class_name=None):
        agents_to_search = [agent] if agent else self.get_available_agents()
        for agent_name in agents_to_search:
            for klass, data in (
                self._agents.get(agent_name, {}).get("classes", {}).items()
            ):
                if class_name and klass != class_name:
                    continue
                if instance in data.get("instances", []):
                    return agent_name, klass
        raise ValueError(f"Instance '{instance}' could not be found.")

    async def _call_static_remote_method(self, agent, class_name, function_name, args):
        topic = f"{self.domain}/{agent}/{class_name}/__static__/{function_name}"
        return await self._execute_remote_call(
            topic, {"c": class_name, "f": function_name, "a": args}
        )

    async def _call_remote_method(self, agent, instance, function_name, args):
        _, class_name = self._find_instance_info(instance, agent=agent)
        topic = f"{self.domain}/{agent}/{class_name}/{instance}/{function_name}"
        return await self._execute_remote_call(
            topic, {"c": instance, "f": function_name, "a": args}
        )

    async def _execute_remote_call(self, topic, json_obj):
        self._invoke_id += 1
        call_id = f"{self._instance}-{self._invoke_id}"
        payload = {
            **json_obj,
            "i": call_id,
            "s": self.vrpc_client_id,
            "v": VRPC_PROTOCOL_VERSION,
        }
        future = asyncio.get_event_loop().create_future()
        self._pending_calls[call_id] = future
        self._client.publish(topic, json.dumps(payload), qos=self.qos)
        try:
            return await asyncio.wait_for(future, self.timeout)
        except asyncio.TimeoutError:
            self._pending_calls.pop(call_id, None)
            raise TimeoutError(f"Call to topic {topic} timed out (> {self.timeout}s)")
