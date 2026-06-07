# tests/agent/test_agent.py

import asyncio
from contextlib import suppress

import aiomqtt
import pytest

from vrpc.adapter import VrpcAdapter
from vrpc.agent import VrpcAgent
from vrpc.client import VrpcClient


# This is the equivalent of the Foo class in the JS test
class Foo:
    def ping(self):
        return "pong"


class EventTestClass:
    """A target class for testing remote callbacks and continuous events."""

    def __init__(self):
        self._cb = None

    def on(self, event_name, callback):
        self._cb = callback

    def off(self, event_name, callback):
        self._cb = None

    def trigger(self, value):
        if self._cb:
            self._cb(value)

    def do_callback(self, callback):
        callback("direct callback")


# This fixture ensures a clean adapter and registers the class for all tests
@pytest.fixture(autouse=True)
def clean_adapter():
    VrpcAdapter._function_registry.clear()
    VrpcAdapter._instances.clear()
    VrpcAdapter._listeners.clear()
    VrpcAdapter.register(Foo)
    VrpcAdapter.register(EventTestClass)
    yield
    VrpcAdapter._function_registry.clear()
    VrpcAdapter._instances.clear()
    VrpcAdapter._listeners.clear()


class TestAgentConstructionAndConnection:
    def test_should_not_construct_using_bad_parameters(self):
        with pytest.raises(
            ValueError, match="Domain must NOT contain any of those characters"
        ):
            VrpcAgent(domain="*")
        with pytest.raises(
            ValueError, match="Agent must NOT contain any of those characters"
        ):
            VrpcAgent(agent="#")
        with pytest.raises(ValueError, match="Domain must be specified"):
            VrpcAgent(domain=None)

    @pytest.mark.asyncio
    async def test_should_not_connect_when_constructed_using_bad_broker(self):
        agent = VrpcAgent(
            broker="mqtt://doesNotExist:1883", domain="test.vrpc", agent="agent1"
        )
        with pytest.raises(aiomqtt.MqttError):
            await asyncio.wait_for(agent.serve(), timeout=2.0)

    @pytest.mark.asyncio
    async def test_should_not_connect_with_wrong_credentials(self):
        agent = VrpcAgent(
            broker="mqtt://broker:1883",
            domain="test.vrpc",
            agent="agent1",
            username="does",
            password="not exist",
        )
        with pytest.raises(aiomqtt.MqttError):
            await asyncio.wait_for(agent.serve(), timeout=2.0)

    @pytest.mark.asyncio
    class TestWithGoodBroker:
        async def test_should_connect_and_end(self):
            agent = VrpcAgent(
                broker="mqtt://broker:1883",
                domain="test.vrpc",
                agent="agent1",
                username="Erwin",
                password="12345",
            )
            connect_event = asyncio.Event()
            agent.on("connect", connect_event.set)

            serve_task = asyncio.create_task(agent.serve())

            try:
                await asyncio.wait_for(connect_event.wait(), timeout=5)
            finally:
                await agent.end()
                serve_task.cancel()
                with suppress(asyncio.CancelledError):
                    await serve_task

        async def test_should_connect_with_custom_client_id(self):
            agent = VrpcAgent(
                broker="mqtt://broker:1883",
                domain="test.vrpc",
                agent="agent1",
                username="Erwin",
                password="12345",
                mqtt_client_id="myMqttClientId",
            )
            assert agent.mqtt_client_id == "myMqttClientId"

            connect_event = asyncio.Event()
            agent.on("connect", connect_event.set)
            serve_task = asyncio.create_task(agent.serve())

            try:
                await asyncio.wait_for(connect_event.wait(), timeout=5)
            finally:
                await agent.end()
                serve_task.cancel()
                with suppress(asyncio.CancelledError):
                    await serve_task


# --- Tests requiring VrpcClient ---


@pytest.mark.asyncio
class TestClientGone:
    async def test_should_signal_when_involved_client_is_gone(self):
        agent = VrpcAgent(
            broker="mqtt://broker:1883",
            domain="test.vrpc",
            agent="agent2",
            username="Erwin",
            password="12345",
        )
        client1 = VrpcClient(
            domain="test.vrpc",
            broker="mqtt://broker:1883",
            username="Erwin",
            password="12345",
        )
        client2 = VrpcClient(
            domain="test.vrpc",
            broker="mqtt://broker:1883",
            username="Erwin",
            password="12345",
        )

        serve_task = asyncio.create_task(agent.serve())

        try:
            await client1.connect()
            await client2.connect()

            # Wait for agent to be online
            await asyncio.sleep(0.5)

            await client2.create(agent="agent2", class_name="Foo", instance="foo")

            client_gone_event = asyncio.Event()
            gone_client_id = None

            def on_client_gone(client_id):
                nonlocal gone_client_id
                gone_client_id = client_id
                client_gone_event.set()

            agent.on("clientGone", on_client_gone)

            await client1.end()
            await asyncio.sleep(0.5)  # Give time for will to propagate
            assert not client_gone_event.is_set()

            await client2.end()
            await asyncio.wait_for(client_gone_event.wait(), timeout=2)
            assert gone_client_id == client2.get_client_id()
        finally:
            # FIX: Clean up all tasks robustly
            serve_task.cancel()
            await client1.end()
            await client2.end()
            with suppress(asyncio.CancelledError):
                await serve_task


@pytest.mark.asyncio
class TestLocalInstanceCreation:
    async def test_should_be_possible_to_create_instance_using_agent(self):
        agent = VrpcAgent(
            broker="mqtt://broker:1883",
            domain="test.vrpc",
            agent="agent3",
            username="Erwin",
            password="12345",
        )
        client = VrpcClient(
            domain="test.vrpc",
            broker="mqtt://broker:1883",
            username="Erwin",
            password="12345",
        )

        serve_task = asyncio.create_task(agent.serve())

        try:
            await client.connect()

            instance_new_event = asyncio.Event()
            client.on("instanceNew", lambda *a, **kw: instance_new_event.set())

            agent.create(class_name="Foo", instance="locallyCreatedFoo")

            await asyncio.wait_for(instance_new_event.wait(), timeout=2)

            proxy = await client.get_instance("locallyCreatedFoo")
            result = await proxy.ping()
            assert result == "pong"
        finally:
            # FIX: Clean up all tasks robustly
            await client.end()
            await agent.end()
            serve_task.cancel()
            with suppress(asyncio.CancelledError):
                await serve_task


@pytest.mark.asyncio
class TestRemoteCallbacksAndEvents:
    async def test_remote_callbacks_and_continuous_events(self):
        agent = VrpcAgent(
            broker="mqtt://broker:1883",
            domain="test.vrpc",
            agent="agent-event-test",
            username="Erwin",
            password="12345",
        )
        client = VrpcClient(
            domain="test.vrpc",
            broker="mqtt://broker:1883",
            username="Erwin",
            password="12345",
        )

        serve_task = asyncio.create_task(agent.serve())

        try:
            await client.connect()

            # Wait briefly to ensure the agent is online and classInfo is populated
            await asyncio.sleep(0.5)

            # 1. Create a shared instance
            proxy = await client.create(
                agent="agent-event-test",
                class_name="EventTestClass",
                instance="emitter1",
            )

            # ---------------------------------------------------------
            # TEST A: One-time Callback (__f__)
            # ---------------------------------------------------------
            callback_received = asyncio.Event()
            callback_value = None

            def one_time_cb(val):
                nonlocal callback_value
                callback_value = val
                callback_received.set()

            # Pass the python function to the remote method
            await proxy.do_callback(one_time_cb)

            # Wait for the MQTT roundtrip
            await asyncio.wait_for(callback_received.wait(), timeout=2.0)
            assert callback_value == "direct callback"

            # ---------------------------------------------------------
            # TEST B: Continuous Event Emitter (__e__)
            # ---------------------------------------------------------
            event_received = asyncio.Event()
            event_value = None

            def continuous_cb(val):
                nonlocal event_value
                event_value = val
                event_received.set()

            # Register the listener
            await proxy.on("my_event", continuous_cb)

            # Trigger the event remotely
            await proxy.trigger("event payload")

            # Wait for the MQTT roundtrip
            await asyncio.wait_for(event_received.wait(), timeout=2.0)
            assert event_value == "event payload"

            # ---------------------------------------------------------
            # TEST C: Continuous Event Removal (off)
            # ---------------------------------------------------------
            event_received.clear()
            event_value = None

            # Remove the listener
            await proxy.off("my_event", continuous_cb)

            # Trigger the event remotely again
            await proxy.trigger("ghost payload")

            # Allow a short buffer to ensure no ghost messages arrive
            await asyncio.sleep(0.5)

            # The event should NOT have been set, and the value should be unchanged
            assert not event_received.is_set()
            assert event_value is None

        finally:
            await client.end()
            await agent.end()
            serve_task.cancel()
            with suppress(asyncio.CancelledError):
                await serve_task


@pytest.mark.asyncio
class TestCallAllExecution:
    async def test_call_all_broadcasts_to_shared_instances(self):
        agent = VrpcAgent(
            broker="mqtt://broker:1883",
            domain="test.vrpc",
            agent="agent-callall-test",
            username="Erwin",
            password="12345",
        )
        client = VrpcClient(
            domain="test.vrpc",
            broker="mqtt://broker:1883",
            username="Erwin",
            password="12345",
        )

        serve_task = asyncio.create_task(agent.serve())

        try:
            await client.connect()

            # Wait briefly to ensure the agent is online
            await asyncio.sleep(0.5)

            # 1. Create two shared instances
            await client.create(
                agent="agent-callall-test", class_name="Foo", instance="foo-1"
            )
            await client.create(
                agent="agent-callall-test", class_name="Foo", instance="foo-2"
            )

            # Wait a tiny bit for the MQTT instanceNew/classInfo updates to sync
            await asyncio.sleep(0.5)

            # 2. Trigger the batch execution
            results = await client.call_all(
                agent="agent-callall-test", class_name="Foo", function_name="ping"
            )

            # 3. Verify the aggregated results
            assert len(results) == 2

            # Map results by instance ID for easy checking
            result_map = {res["id"]: res["val"] for res in results}

            assert result_map["foo-1"] == "pong"
            assert result_map["foo-2"] == "pong"
            assert all(res["err"] is None for res in results)

        finally:
            await client.end()
            await agent.end()
            serve_task.cancel()
            with suppress(asyncio.CancelledError):
                await serve_task
