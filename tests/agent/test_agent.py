# tests/agent/test_agent.py

import asyncio

import pytest
import pytest_asyncio

from vrpc.adapter import VrpcAdapter
from vrpc.agent import VrpcAgent
from vrpc.client import VrpcClient


class Foo:
    def ping(self):
        return "pong"


@pytest.fixture(autouse=True)
def clean_adapter():
    VrpcAdapter._function_registry.clear()
    VrpcAdapter._instances.clear()
    VrpcAdapter.register(Foo)
    yield
    VrpcAdapter._function_registry.clear()
    VrpcAdapter._instances.clear()


class TestConstructionAndConnection:
    def test_should_not_construct_using_bad_parameters(self):
        with pytest.raises(ValueError, match="Domain must be specified"):
            VrpcAgent(domain=None)
        with pytest.raises(ValueError, match="must NOT contain"):
            VrpcAgent(domain="*")
        with pytest.raises(ValueError, match="Agent must be specified"):
            VrpcAgent(agent=None)
        with pytest.raises(ValueError, match="must NOT contain"):
            VrpcAgent(agent="a/b")

    @pytest.mark.asyncio
    async def test_should_not_connect_with_wrong_credentials(self, mocker):
        connect_spy = mocker.spy(VrpcAgent, "emit")
        agent = VrpcAgent(
            agent="test-agent",
            broker="mqtt://broker:1883",
            domain="test.vrpc",
            username="WrongUser",
            password="WrongPassword",
        )
        serve_task = asyncio.create_task(agent.serve())
        await asyncio.sleep(0.5)
        assert not any(call.args[0] == "connect" for call in connect_spy.call_args_list)
        await agent.end()
        serve_task.cancel()

    @pytest.mark.asyncio
    async def test_should_connect_and_end_cleanly(self, mocker):
        connect_spy = mocker.spy(VrpcAgent, "emit")
        agent = VrpcAgent(
            broker="mqtt://broker:1883",
            domain="test.vrpc",
            agent="agent1",
            username="Erwin",
            password="12345",
        )
        serve_task = asyncio.create_task(agent.serve())
        await asyncio.sleep(0.2)
        assert any(call.args[0] == "connect" for call in connect_spy.call_args_list)
        assert agent._client.is_connected
        await agent.end()
        serve_task.cancel()
        assert not agent._client.is_connected

    @pytest.mark.asyncio
    async def test_should_connect_with_custom_client_id(self):
        agent = VrpcAgent(
            broker="mqtt://broker:1883",
            domain="test.vrpc",
            agent="agent1",
            username="Erwin",
            password="12345",
            mqtt_client_id="myMqttClientId",
        )
        serve_task = asyncio.create_task(agent.serve())
        await asyncio.sleep(0.2)
        assert agent.mqtt_client_id == "myMqttClientId"
        await agent.end()
        serve_task.cancel()


@pytest_asyncio.fixture
async def agent_and_clients():
    agent = VrpcAgent(
        broker="mqtt://broker:1883",
        domain="test.vrpc",
        agent="agent2",
        username="Erwin",
        password="12345",
    )
    agent_task = asyncio.create_task(agent.serve())
    client1 = VrpcClient(
        broker="mqtt://broker:1883",
        domain="test.vrpc",
        username="Erwin",
        password="12345",
    )
    client2 = VrpcClient(
        broker="mqtt://broker:1883",
        domain="test.vrpc",
        username="Erwin",
        password="12345",
    )
    await asyncio.gather(
        agent._ensure_connected(), client1.connect(), client2.connect()
    )
    yield agent, client1, client2
    await agent.end()
    agent_task.cancel()
    try:
        await client1.end()
        await client2.end()
    except ConnectionError:
        pass


class TestClientLifecycle:
    @pytest.mark.asyncio
    async def test_should_signal_when_an_involved_client_is_gone(
        self, mocker, agent_and_clients
    ):
        agent, client1, client2 = agent_and_clients
        client_gone_spy = mocker.spy(agent, "emit")
        await client2.create(agent="agent2", class_name="Foo", is_isolated=True)
        await asyncio.sleep(0.1)
        await client1.end()
        await asyncio.sleep(0.2)
        assert not any(
            x.args[0] == "clientGone" for x in client_gone_spy.call_args_list
        )
        await client2.end()
        await asyncio.sleep(0.2)
        assert any(
            x.args == ("clientGone", client2.get_client_id())
            for x in client_gone_spy.call_args_list
        )


@pytest_asyncio.fixture
async def agent_and_client():
    agent = VrpcAgent(
        broker="mqtt://broker:1883",
        domain="test.vrpc",
        agent="agent3",
        username="Erwin",
        password="12345",
    )
    agent_task = asyncio.create_task(agent.serve())
    client = VrpcClient(
        broker="mqtt://broker:1883",
        domain="test.vrpc",
        username="Erwin",
        password="12345",
    )
    await asyncio.gather(agent._ensure_connected(), client.connect())
    yield agent, client
    await client.end()
    await agent.end()
    agent_task.cancel()


class TestLocalInstanceCreation:
    @pytest.mark.asyncio
    async def test_agent_can_create_an_instance_locally(self, mocker, agent_and_client):
        agent, client = agent_and_client
        instance_new_spy = mocker.spy(client, "emit")
        # FIX: Call agent.create() instead of VrpcAdapter.create()
        agent.create(class_name="Foo", instance="locallyCreatedFoo")
        await asyncio.sleep(0.2)
        proxy = await client.get_instance("locallyCreatedFoo")
        value = await proxy.ping()
        assert value == "pong"
        assert any(
            call.args[0] == "instanceNew" for call in instance_new_spy.call_args_list
        )
