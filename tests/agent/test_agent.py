# tests/agent/test_agent.py

import asyncio

import aiomqtt
import pytest

from vrpc.adapter import VrpcAdapter
from vrpc.agent import VrpcAgent


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
                with pytest.raises(asyncio.CancelledError):
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
                with pytest.raises(asyncio.CancelledError):
                    await serve_task


# --- Tests requiring VrpcClient (skipped for now) ---


@pytest.mark.skip(reason="Requires VrpcClient implementation")
@pytest.mark.asyncio
class TestClientGone:
    async def test_should_signal_when_involved_client_is_gone(self):
        # This test will require the VrpcClient class
        pass


@pytest.mark.skip(reason="Requires VrpcClient implementation")
@pytest.mark.asyncio
class TestLocalInstanceCreation:
    async def test_should_be_possible_to_create_instance_using_agent(self):
        # This test will require the VrpcClient class
        pass
