# vrpc/agent.py

import asyncio
import hashlib
import json
import logging
import os
import platform
from argparse import ArgumentParser
from collections import defaultdict

from gmqtt import Client as MqttClient
from gmqtt import Message

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
        self._validate_domain(domain)
        self._validate_agent(agent)

        self.agent = agent or self._generate_agent_name()
        self.domain = domain
        self.broker = broker
        self.username = username
        self.password = password
        self.token = token
        self.version = version
        self.qos = 0 if best_effort else 1
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
        username = self.username
        password = self.password
        if self.token:
            username = f"{self.domain}/{self.agent}"
            password = self.token

        self._client = MqttClient(self.mqtt_client_id)
        self._client.set_auth_credentials(username, password)

        self._client.will_message = Message(
            topic=f"{self.base_topic}/__agentInfo__",
            payload=self._create_agent_info_payload(status="offline"),
            qos=self.qos,
            retain=True,
        )

        self._client.on_connect = self._handle_connect
        self._client.on_message = self._handle_message
        self._client.on_disconnect = lambda *args: self.emit("close")

        host, port_str = (
            self.broker.replace("mqtts://", "").replace("mqtt://", "").split(":")
        )
        logger.info(f"Connecting agent '{self.agent}' to {self.broker}...")
        await self._client.connect(
            host, int(port_str), ssl=self.broker.startswith("mqtts")
        )
        await asyncio.Event().wait()

    async def end(self, unregister: bool = False):
        """Stops the agent and disconnects from the broker."""
        if not self._client or not self._client.is_connected:
            self.emit("end")
            return

        agent_info_topic = f"{self.base_topic}/__agentInfo__"
        self._client.publish(
            agent_info_topic,
            self._create_agent_info_payload(status="offline"),
            qos=self.qos,
            retain=True,
        )
        if unregister:
            self._client.publish(agent_info_topic, "", qos=self.qos, retain=True)
            for class_name in VrpcAdapter.get_available_classes():
                class_info_topic = f"{self.base_topic}/{class_name}/__classInfo__"
                concise_topic = f"{self.base_topic}/{class_name}/__classInfoConcise__"
                self._client.publish(class_info_topic, "", qos=self.qos, retain=True)
                self._client.publish(concise_topic, "", qos=self.qos, retain=True)
        await self._client.disconnect()
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
        if self._client and self._client.is_connected:
            self._publish_class_info(class_name)
            self._publish_class_info_concise(class_name)
        return obj

    async def _ensure_connected(self):
        """Waits until the client is connected."""
        if self._client and self._client.is_connected:
            return
        connected_future = asyncio.get_event_loop().create_future()

        def on_connect(*args, **kwargs):
            if not connected_future.done():
                connected_future.set_result(True)

        self.on("connect", on_connect)
        await connected_future
        self.off("connect", on_connect)

    def _handle_connect(self, client, flags, rc, properties):
        """Callback on successful connection to the MQTT broker."""
        logger.info("Agent connected successfully.")
        self.emit("connect")
        topics = self._generate_topics()
        for topic in topics:
            client.subscribe(topic, qos=self.qos)
        for class_name in VrpcAdapter.get_available_classes():
            for instance_name in VrpcAdapter.get_available_instances(class_name):
                self._subscribe_to_instance_methods(class_name, instance_name)
        self._publish_agent_info()
        for class_name in VrpcAdapter.get_available_classes():
            self._publish_class_info(class_name)
            self._publish_class_info_concise(class_name)

    def _handle_message(self, client, topic, payload, qos, properties):
        """Callback for incoming MQTT messages."""
        logger.debug(f"Agent received message on topic: {topic}")
        try:
            json_obj = json.loads(payload)
            tokens = topic.split("/")
            if len(tokens) >= 4 and tokens[3] == "__clientInfo__":
                self._handle_client_info_message(topic, json_obj)
                return
            _, _, class_name, instance, method = tokens
            json_obj["c"] = class_name if instance == "__static__" else instance
            json_obj["f"] = method
            VrpcAdapter.call(json.dumps(json_obj))
            if method == "__createIsolated__":
                instance_name = json_obj.get("r")
                client_id = json_obj.get("s")
                if instance_name and client_id:
                    self._subscribe_to_instance_methods(class_name, instance_name)
                    self._register_isolated_instance(instance_name, client_id)
            elif method == "__createShared__":
                instance_name = json_obj.get("r")
                if instance_name:
                    self._subscribe_to_instance_methods(class_name, instance_name)
                    self._publish_class_info(class_name)
                    self._publish_class_info_concise(class_name)
            elif method == "__delete__":
                instance_name = json_obj.get("a", [None])[0]
                if instance_name:
                    self._unsubscribe_from_instance_methods(class_name, instance_name)
                    self._publish_class_info(class_name)
                    self._publish_class_info_concise(class_name)
        except Exception as e:
            logger.error(
                f"Failed to handle message on topic {topic}: {e}", exc_info=True
            )

    def _handle_client_info_message(self, topic, json_obj):
        """Handles a client's status message (e.g., going offline)."""
        if json_obj.get("status") == "offline":
            client_id = "/".join(topic.split("/")[1:-1])
            logger.info(f"Client '{client_id}' went offline, cleaning up resources.")
            if client_id in self._isolated_instances:
                for instance_id in self._isolated_instances[client_id]:
                    VrpcAdapter.delete(instance_id)
                del self._isolated_instances[client_id]
            self._client.unsubscribe(f"{self.domain}/{client_id}/__clientInfo__")
            self.emit("clientGone", client_id)

    def _register_isolated_instance(self, instance_id, client_id):
        """Tracks an isolated instance and the client who created it."""
        if (
            not self._isolated_instances[client_id]
            and not self._shared_instances[client_id]
        ):
            self._client.subscribe(
                f"{self.domain}/{client_id}/__clientInfo__", qos=self.qos
            )
            logger.info(f"Tracking lifetime of client: {client_id}")
        self._isolated_instances[client_id].add(instance_id)

    def _handle_vrpc_callback(self, data: dict):
        """Callback from VrpcAdapter to send results back to the caller."""
        sender_id = data.get("s")
        if not sender_id:
            return
        topic = f"{self.domain}/{sender_id}/-/client/callback"
        payload = json.dumps(
            {k: v for k, v in data.items() if k in ("a", "r", "e", "i")}
        )
        self._client.publish(topic, payload, qos=self.qos)

    def _generate_topics(self):
        """Generates a list of topics for all static functions."""
        topics = []
        for class_name in VrpcAdapter.get_available_classes():
            for func in VrpcAdapter._function_registry.get(class_name, {}).get(
                "static_functions", []
            ):
                topics.append(f"{self.base_topic}/{class_name}/__static__/{func}")
        return topics

    def _subscribe_to_instance_methods(self, class_name, instance_name):
        """Subscribes to all public methods of a new class instance."""
        self._client.subscribe(
            f"{self.base_topic}/{class_name}/{instance_name}/+", qos=self.qos
        )

    def _unsubscribe_from_instance_methods(self, class_name, instance_name):
        """Unsubscribes from an instance's methods upon deletion."""
        self._client.unsubscribe(f"{self.base_topic}/{class_name}/{instance_name}/+")

    def _publish_agent_info(self):
        """Publishes the agent's online status and metadata."""
        payload = self._create_agent_info_payload(status="online")
        self._client.publish(
            f"{self.base_topic}/__agentInfo__", payload, qos=self.qos, retain=True
        )

    def _publish_class_info(self, class_name):
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
        self._client.publish(
            f"{self.base_topic}/{class_name}/__classInfo__",
            json.dumps(payload),
            qos=self.qos,
            retain=True,
        )

    def _publish_class_info_concise(self, class_name):
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
        self._client.publish(
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
        if not agent:
            raise ValueError("Agent must be specified")
        if any(c in agent for c in "+/#*"):
            raise ValueError(
                'Agent must NOT contain any of those characters: "+, /, #, *"'
            )
