# tests/test_adapter.py

import pytest

from tests.adapter.fixtures.sample_class_no_doc import SampleClassNoDoc

# Assuming your vrpc package is in the project root
from vrpc.adapter import VrpcAdapter


# This is a pytest fixture. It will run before each test function that uses it.
# It ensures that the VrpcAdapter's registries are empty for each test,
# preventing tests from interfering with each other.
@pytest.fixture(autouse=True)
def clean_adapter():
    # Setup: runs before the test
    VrpcAdapter._function_registry.clear()
    VrpcAdapter._instances.clear()
    yield
    # Teardown: runs after the test
    VrpcAdapter._function_registry.clear()
    VrpcAdapter._instances.clear()


# --- Auto-registration Tests ---


def test_manual_registration_of_a_class_given_a_class():
    VrpcAdapter.register(SampleClassNoDoc)
    assert VrpcAdapter.get_available_classes() == ["SampleClassNoDoc"]


def test_manual_registration_of_a_class_given_a_path():
    # pytest automatically makes the path relative to the root
    VrpcAdapter.register("tests/adapter/fixtures/sample_class_doc.py")
    assert VrpcAdapter.get_available_classes() == ["SampleClassDoc"]


def test_manual_registration_of_an_instance():
    test_instance = SampleClassNoDoc(42)
    VrpcAdapter.register_instance(
        test_instance, class_name="SampleClassNoDoc", instance="noDoc1"
    )
    assert VrpcAdapter.get_available_instances("SampleClassNoDoc") == ["noDoc1"]


# --- Creation / Deletion and Availability Tests ---


def test_creation_should_not_create_instance_of_non_existing_class():
    with pytest.raises(ValueError, match='"DoesNotExist" is not a registered class'):
        VrpcAdapter.create(class_name="DoesNotExist")


def test_creation_with_minimal_parameters():
    VrpcAdapter.register(SampleClassNoDoc)
    # In Python, we can use a simple list to "spy" on events
    create_events = []
    VrpcAdapter._emitter.on("create", lambda data: create_events.append(data))

    instance = VrpcAdapter.create(class_name="SampleClassNoDoc")

    assert instance.get_value() == 0
    # One instance was created automatically
    assert len(VrpcAdapter.get_available_instances("SampleClassNoDoc")) == 1

    # Check event emission
    assert len(create_events) == 1
    event_data = create_events[0]
    assert event_data["className"] == "SampleClassNoDoc"
    assert not event_data["isIsolated"]


def test_creation_with_specific_instance_name_and_args():
    VrpcAdapter.register(SampleClassNoDoc)
    instance = VrpcAdapter.create(
        class_name="SampleClassNoDoc", instance="myInstance1", args=[42]
    )
    assert instance.get_value() == 42
    assert VrpcAdapter.get_available_instances("SampleClassNoDoc") == ["myInstance1"]


def test_creation_in_isolated_mode():
    VrpcAdapter.register(SampleClassNoDoc)
    VrpcAdapter.create(class_name="SampleClassNoDoc", instance="myInstance1", args=[42])
    VrpcAdapter.create(
        class_name="SampleClassNoDoc",
        instance="isolatedInstance",
        args=[-1],
        is_isolated=True,
    )
    # Isolated instances should not appear in the public list
    assert VrpcAdapter.get_available_instances("SampleClassNoDoc") == ["myInstance1"]
    # But the instance should exist internally
    assert len(VrpcAdapter._instances) == 2


def test_deletion_of_an_instance():
    VrpcAdapter.register(SampleClassNoDoc)
    VrpcAdapter.create(class_name="SampleClassNoDoc", instance="instance-to-delete")
    assert len(VrpcAdapter._instances) == 1

    # Test deletion
    was_deleted = VrpcAdapter.delete("instance-to-delete")
    assert was_deleted
    assert len(VrpcAdapter._instances) == 0

    # Test deleting a non-existent instance
    was_not_deleted = VrpcAdapter.delete("does-not-exist")
    assert not was_not_deleted


# --- Documentation Parsing Tests ---


def test_documentation_parsing_for_parity_with_js():
    VrpcAdapter.register("tests/adapter/fixtures/sample_class_doc.py")
    meta = VrpcAdapter.get_meta_data("SampleClassDoc")

    assert sorted(list(meta.keys())) == sorted(
        ["__createShared__", "get_value", "set_value"]
    )

    constructor_meta = meta["__createShared__"]
    assert constructor_meta["description"] == "Constructor"
    assert len(constructor_meta["params"]) == 2

    # Check injected instanceName parameter
    assert constructor_meta["params"][0]["name"] == "instanceName"
    assert constructor_meta["params"][0]["type"] == "string"
    assert constructor_meta["params"][0]["optional"] is False

    # Check original constructor parameter
    assert constructor_meta["params"][1]["name"] == "value"
    assert constructor_meta["params"][1]["type"] == "int"
    assert constructor_meta["params"][1]["default"] == "0"
    assert constructor_meta["params"][1]["optional"] is True

    # Check set_value metadata
    set_value_meta = meta["set_value"]
    assert set_value_meta["description"] == "Sets a value"
    assert len(set_value_meta["params"]) == 1
    assert set_value_meta["params"][0]["name"] == "value"
    assert set_value_meta["ret"]["type"] == "int"
    assert set_value_meta["ret"]["description"] == "the updated value"


# --- Event Listener and Callbacks Tests  ---


class DummyEmitter:
    """A simple mock class simulating an event emitter."""

    def __init__(self):
        self.callbacks = {}

    def on(self, event, callback):
        self.callbacks[event] = callback

    def off(self, event, callback):
        if event in self.callbacks:
            del self.callbacks[event]

    def remove_all_listeners(self, event):
        if event in self.callbacks:
            del self.callbacks[event]

    def trigger(self, event, *args):
        if event in self.callbacks and self.callbacks[event]:
            self.callbacks[event](*args)


def test_adapter_registers_and_triggers_event_listener():
    VrpcAdapter.register(DummyEmitter)
    instance = VrpcAdapter.create("DummyEmitter", instance="emitter1")

    # Mock the global callback used to send MQTT messages back
    callbacks_received = []
    VrpcAdapter.on_callback(lambda data: callbacks_received.append(data))

    # Simulate the RPC call to register a listener (like .on("data", callback))
    json_call = {
        "c": "emitter1",
        "f": "on",
        "a": ["data", "__e__topic123"],
        "s": "client-1",
        "i": "msg-1",
    }
    must_track = VrpcAdapter._call(json_call)

    # Adapter should report that we need to track this client's lifecycle
    assert must_track is True
    assert "emitter1" in VrpcAdapter._listeners
    assert "__e__topic123" in VrpcAdapter._listeners["emitter1"]

    # Trigger the event locally on the Python instance
    instance.trigger("data", 42, "hello")

    # Check if the global callback caught it with the correct VRPC payload
    assert len(callbacks_received) == 1
    assert callbacks_received[0]["i"] == "__e__topic123"
    assert callbacks_received[0]["a"] == [42, "hello"]


def test_adapter_unregisters_event_listener():
    VrpcAdapter.register(DummyEmitter)
    VrpcAdapter.create("DummyEmitter", instance="emitter1")

    # Register the listener
    VrpcAdapter._call(
        {
            "c": "emitter1",
            "f": "on",
            "a": ["data", "__e__topic123"],
            "s": "client-1",
            "i": "msg-1",
        }
    )

    assert "__e__topic123" in VrpcAdapter._listeners["emitter1"]

    # Simulate the RPC call to remove the listener (like .off("data", callback))
    must_track = VrpcAdapter._call(
        {
            "c": "emitter1",
            "f": "off",
            "a": ["data", "__e__topic123"],
            "s": "client-1",
            "i": "msg-2",
        }
    )

    # off() does not require tracking a new lifecycle
    assert must_track is False
    assert "emitter1" not in VrpcAdapter._listeners


def test_adapter_unregisters_client_on_drop():
    VrpcAdapter.register(DummyEmitter)
    VrpcAdapter.create("DummyEmitter", instance="emitter1")

    # Register a listener for client-1
    VrpcAdapter._call(
        {
            "c": "emitter1",
            "f": "on",
            "a": ["data", "__e__topic123"],
            "s": "client-1",
            "i": "msg-1",
        }
    )

    assert "emitter1" in VrpcAdapter._listeners

    # Simulate client-1 dropping off the MQTT broker unexpectedly
    # This mimics the Agent catching the '__clientInfo__' offline message
    VrpcAdapter._unregister_client("client-1")

    # The adapter should have safely wiped the memory and unregistered the function
    assert "emitter1" not in VrpcAdapter._listeners

    # --- __callAll__ Execution Tests  ---


class CallAllTarget:
    def __init__(self, value):
        self.value = value

    def get_value(self):
        return self.value


def test_adapter_executes_call_all():
    VrpcAdapter.register(CallAllTarget)

    # Create two shared instances and one isolated instance
    VrpcAdapter.create("CallAllTarget", instance="target1", args=[10])
    VrpcAdapter.create("CallAllTarget", instance="target2", args=[20])
    VrpcAdapter.create(
        "CallAllTarget", instance="target-isolated", args=[99], is_isolated=True
    )

    json_call = {
        "c": "CallAllTarget",
        "f": "__callAll__",
        "a": ["get_value"],  # First arg is the method to execute
        "s": "client-1",
        "i": "msg-1",
    }

    VrpcAdapter._call(json_call)

    # Results should be stored in 'r'
    results = json_call.get("r")
    assert results is not None
    assert len(results) == 2  # The isolated instance must be excluded!

    # Check that both shared instances returned their correct values
    result_map = {res["id"]: res["val"] for res in results}
    assert result_map["target1"] == 10
    assert result_map["target2"] == 20
    assert all(res["err"] is None for res in results)
