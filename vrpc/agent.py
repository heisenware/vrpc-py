# vrpc/agent.py

import asyncio
import hashlib
import json
import logging
import os
import platform
from argparse import ArgumentParser
from collections import defaultdict

import aiomqtt

from .adapter import EventEmitter, VrpcAdapter

logger = logging.getLogger(__name__)

VRPC_PROTOCOL_VERSION = 3


class VrpcAgent(EventEmitter):
    """
    Agent capable of making existing code available for remote control.
    This class mirrors the logic and capabilities of VrpcAgent.js.
    """

    def __init__(
        self,
        agent: str = None,
        domain: str = "vrpc",
        broker: str = "mqtts://vrpc.io:8883",
        username: str = None,
        password: str = None,
        token: str = None,
        version: str = "",
        best_effort: bool = True,
        mqtt_client_id: str = None,
    ):
        super().__init__()

        self.agent = agent or self._generate_agent_name()
        self.domain = domain
        self.broker = broker
        self.username = username
        self.password = password
        self.token = token
        self.version = version
        self.qos = 0 if best_effort else 1

        self._validate_domain(self.domain)
        self._validate_agent(self.agent)

        self.mqtt_client_id = mqtt_client_id or self._generate_mqtt_client_id()
        self.base_topic = f"{self.domain}/{self.agent}"

        self._client = None
        self._isolated_instances = defaultdict(set)
        self._shared_instances = defaultdict(set)

        VrpcAdapter.on_callback(self._handle_vrpc_callback)
        self.on("error", lambda err: logger.debug(f"Agent error: {err}"))

    @staticmethod
    def from_commandline(defaults: dict = None):
        """Constructs an agent by parsing command line arguments."""
        defaults = defaults or {}
        parser = ArgumentParser(description="VRPC Python Agent", add_help=True)
        parser.add_argument(
            "-a", "--agent", help="Agent name", default=defaults.get("agent")
        )
        parser.add_argument(
            "-d", "--domain", help="Domain name", default=defaults.get("domain", "vrpc")
        )
        parser.add_argument(
            "-b",
            "--broker",
            help="Broker URL",
            default=defaults.get("broker", "mqtts://vrpc.io:8883"),
        )
        parser.add_argument(
            "-u", "--username", help="MQTT username", default=defaults.get("username")
        )
        parser.add_argument(
            "-p", "--password", help="MQTT password", default=defaults.get("password")
        )
        parser.add_argument(
            "-t", "--token", help="VRPC access token", default=defaults.get("token")
        )
        parser.add_argument(
            "--best-effort",
            help="Sets MQTT QoS to 0 for improved performance.",
            action="store_true",
            default=defaults.get("best_effort", False),
        )
        parser.add_argument(
            "-v",
            "--version",
            help="User-defined agent version",
            default=defaults.get("version", ""),
        )
        args = parser.parse_args()
        return VrpcAgent(**vars(args))

    async def serve(self):
        """Connects the agent to the broker and starts serving."""
        will = aiomqtt.Will(
            topic=f"{self.base_topic}/__agentInfo__",
            payload=self._create_agent_info_payload(status="offline"),
            qos=self.qos,
            retain=True,
        )
        try:
            async with aiomqtt.Client(
                hostname=self.broker.split("://")[1].split(":")[0],
                port=int(self.broker.split(":")[-1]),
                username=self.username,
                password=self.password,
                identifier=self.mqtt_client_id,  # FIX: Was client_id
                will=will,
                tls_params=aiomqtt.TLSParameters() if "mqtts" in self.broker else None,
            ) as client:
                self._client = client
                await self._handle_connect()
                logger.info(
                    f"Agent '{self.agent}' connected and listening for messages."
                )
                async for message in client.messages:
                    await self._handle_message(message)
        except aiomqtt.MqttError as error:
            logger.error(f"MQTT Connection Error: '{error}'.")
            # Re-raise the error so tests can catch it
            raise

    async def end(self, unregister: bool = False):
        """Stops the agent and disconnects from the broker."""
        if not self._client:
            self.emit("end")
            return
        agent_info_topic = f"{self.base_topic}/__agentInfo__"
        await self._client.publish(
            agent_info_topic,
            payload=self._create_agent_info_payload(status="offline"),
            qos=self.qos,
            retain=True,
        )
        if unregister:
            await self._client.publish(
                agent_info_topic, payload="", qos=self.qos, retain=True
            )
            for class_name in VrpcAdapter.get_available_classes():
                await self._client.publish(
                    f"{self.base_topic}/{class_name}/__classInfo__",
                    payload="",
                    qos=self.qos,
                    retain=True,
                )
                await self._client.publish(
                    f"{self.base_topic}/{class_name}/__classInfoConcise__",
                    payload="",
                    qos=self.qos,
                    retain=True,
                )
        self.emit("end")

    def create(
        self,
        class_name: str,
        instance: str = None,
        args: list = None,
        is_isolated: bool = False,
    ):
        """Creates a new instance locally and notifies clients."""
        obj = VrpcAdapter.create(
            class_name=class_name, instance=instance, args=args, is_isolated=is_isolated
        )
        if self._client:
            asyncio.create_task(self._publish_class_info(class_name))
            asyncio.create_task(self._publish_class_info_concise(class_name))
        return obj

    async def _handle_connect(self):
        """Callback on successful connection to the MQTT broker."""
        self.emit("connect")
        topics = self._generate_topics()
        for topic in topics:
            await self._client.subscribe(topic, qos=self.qos)
        for class_name in VrpcAdapter.get_available_classes():
            for instance_name in VrpcAdapter.get_available_instances(class_name):
                await self._subscribe_to_instance_methods(class_name, instance_name)
        await self._publish_agent_info()
        for class_name in VrpcAdapter.get_available_classes():
            await self._publish_class_info(class_name)
            await self._publish_class_info_concise(class_name)

    async def _handle_message(self, message: aiomqtt.Message):
        """Callback for incoming MQTT messages."""
        topic = message.topic.value
        logger.debug(f"Agent received message on topic: {topic}")
        try:
            json_str = message.payload.decode("utf-8")
            if not json_str:  # Handle empty retained messages on unregister
                return
            json_obj = json.loads(json_str)
            tokens = topic.split("/")

            if len(tokens) >= 4 and tokens[3] == "__clientInfo__":
                await self._handle_client_info_message(topic, json_obj)
                return

            _, _, class_name, instance, method = tokens
            json_obj["c"] = class_name if instance == "__static__" else instance
            json_obj["f"] = method
            mutated_json_str = json.dumps(json_obj)

            result_json_str = VrpcAdapter.call(mutated_json_str)
            result_obj = json.loads(result_json_str)

            is_promise = isinstance(result_obj.get("r"), str) and result_obj[
                "r"
            ].startswith("__p__")
            if not is_promise:
                reply_topic = result_obj.get("s")
                if reply_topic:
                    await self._client.publish(
                        reply_topic, result_json_str, qos=self.qos
                    )

            if method == "__createIsolated__":
                instance_name = result_obj.get("r")
                client_id = result_obj.get("s")
                if instance_name and client_id:
                    await self._subscribe_to_instance_methods(class_name, instance_name)
                    await self._register_isolated_instance(instance_name, client_id)
            elif method == "__createShared__":
                instance_name = result_obj.get("r")
                if instance_name:
                    await self._subscribe_to_instance_methods(class_name, instance_name)
                    await self._publish_class_info(class_name)
                    await self._publish_class_info_concise(class_name)
            elif method == "__delete__":
                instance_name = json_obj.get("a", [None])[0]
                if instance_name:
                    await self._unsubscribe_from_instance_methods(
                        class_name, instance_name
                    )
                    await self._publish_class_info(class_name)
                    await self._publish_class_info_concise(class_name)

        except Exception as e:
            logger.error(
                f"Failed to handle message on topic {topic}: {e}", exc_info=True
            )

    async def _handle_client_info_message(self, topic, json_obj):
        """Handles a client's status message (e.g., going offline)."""
        if json_obj.get("status") == "offline":
            client_id = "/".join(topic.split("/")[1:-1])
            logger.info(f"Client '{client_id}' went offline, cleaning up resources.")
            if client_id in self._isolated_instances:
                for instance_id in self._isolated_instances[client_id]:
                    VrpcAdapter.delete(instance_id)
                del self._isolated_instances[client_id]
            await self._client.unsubscribe(f"{self.domain}/{client_id}/__clientInfo__")
            self.emit("clientGone", client_id)

    async def _register_isolated_instance(self, instance_id, client_id):
        """Tracks an isolated instance and the client who created it."""
        if (
            not self._isolated_instances[client_id]
            and not self._shared_instances[client_id]
        ):
            await self._client.subscribe(
                f"{self.domain}/{client_id}/__clientInfo__", qos=self.qos
            )
            logger.info(f"Tracking lifetime of client: {client_id}")
        self._isolated_instances[client_id].add(instance_id)

    def _handle_vrpc_callback(self, data: dict):
        """Callback from VrpcAdapter to send results back to the caller."""
        topic = data.get("s")
        if not topic:
            topic = data.get("i")
            if not topic or not topic.startswith("__e__"):
                return
            topic = topic[5:]

        payload = json.dumps(
            {k: v for k, v in data.items() if k in ("a", "r", "e", "i", "v")}
        )

        if self._client:
            asyncio.create_task(self._client.publish(topic, payload, qos=self.qos))

    def _generate_topics(self):
        """Generates a list of topics for all static functions."""
        topics = []
        for class_name in VrpcAdapter.get_available_classes():
            entry = VrpcAdapter._function_registry.get(class_name, {})
            for func in entry.get("static_functions", []):
                topics.append(f"{self.base_topic}/{class_name}/__static__/{func}")
            topics.append(
                f"{self.base_topic}/{class_name}/__static__/__createIsolated__"
            )
            topics.append(f"{self.base_topic}/{class_name}/__static__/__createShared__")
            topics.append(f"{self.base_topic}/{class_name}/__static__/__delete__")
            topics.append(f"{self.base_topic}/{class_name}/__static__/__callAll__")
        return topics

    async def _subscribe_to_instance_methods(self, class_name, instance_name):
        """Subscribes to all public methods of a new class instance."""
        await self._client.subscribe(
            f"{self.base_topic}/{class_name}/{instance_name}/+", qos=self.qos
        )

    async def _unsubscribe_from_instance_methods(self, class_name, instance_name):
        """Unsubscribes from an instance's methods upon deletion."""
        await self._client.unsubscribe(
            f"{self.base_topic}/{class_name}/{instance_name}/+"
        )

    async def _publish_agent_info(self):
        """Publishes the agent's online status and metadata."""
        payload = self._create_agent_info_payload(status="online")
        await self._client.publish(
            f"{self.base_topic}/__agentInfo__", payload, qos=self.qos, retain=True
        )

    async def _publish_class_info(self, class_name):
        """Publishes the full information about a registered class."""
        payload = {
            "className": class_name,
            "instances": VrpcAdapter.get_available_instances(class_name),
            "memberFunctions": VrpcAdapter._function_registry.get(class_name, {}).get(
                "member_functions", []
            ),
            "staticFunctions": VrpcAdapter._function_registry.get(class_name, {}).get(
                "static_functions", []
            ),
            "meta": VrpcAdapter.get_meta_data(class_name),
            "v": VRPC_PROTOCOL_VERSION,
        }
        await self._client.publish(
            f"{self.base_topic}/{class_name}/__classInfo__",
            json.dumps(payload),
            qos=self.qos,
            retain=True,
        )

    async def _publish_class_info_concise(self, class_name):
        """Publishes concise information about a registered class."""
        payload = {
            "className": class_name,
            "instances": VrpcAdapter.get_available_instances(class_name),
            "memberFunctions": VrpcAdapter._function_registry.get(class_name, {}).get(
                "member_functions", []
            ),
            "staticFunctions": VrpcAdapter._function_registry.get(class_name, {}).get(
                "static_functions", []
            ),
            "v": VRPC_PROTOCOL_VERSION,
        }
        await self._client.publish(
            f"{self.base_topic}/{class_name}/__classInfoConcise__",
            json.dumps(payload),
            qos=self.qos,
            retain=True,
        )

    def _create_agent_info_payload(self, status: str) -> str:
        """Creates the JSON payload for the __agentInfo__ topic."""
        return json.dumps(
            {
                "status": status,
                "hostname": platform.node(),
                "version": self.version,
                "v": VRPC_PROTOCOL_VERSION,
            }
        )

    def _generate_agent_name(self) -> str:
        """Generates a default agent name."""
        try:
            username = os.getlogin()
        except OSError:
            username = "user"
        path_id = hashlib.md5(os.getcwd().encode()).hexdigest()[:4]
        return f"{username}-{path_id}@{platform.node()}-{platform.system().lower()}-py"

    def _generate_mqtt_client_id(self) -> str:
        """Generates a predictable and unique MQTT client ID."""
        hash_str = hashlib.md5(f"{self.domain}{self.agent}".encode()).hexdigest()
        return f"va3{hash_str[:20]}"

    def _validate_domain(self, domain):
        """Validates the domain string."""
        if not domain:
            raise ValueError("Domain must be specified")
        if any(c in domain for c in "+/#*"):
            raise ValueError(
                'Domain must NOT contain any of those characters: "+, /, #, *"'
            )

    def _validate_agent(self, agent):
        """Validates the agent string."""
        if agent and any(c in agent for c in "+/#*"):
            raise ValueError(
                'Agent must NOT contain any of those characters: "+, /, #, *"'
            )
