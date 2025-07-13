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


# This fixture ensures a clean adapter and registers the class for all tests
@pytest.fixture(autouse=True)
def clean_adapter():
    VrpcAdapter._function_registry.clear()
    VrpcAdapter._instances.clear()
    VrpcAdapter.register(Foo)
    yield
    VrpcAdapter._function_registry.clear()
    VrpcAdapter._instances.clear()


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
