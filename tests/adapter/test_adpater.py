# tests/test_adapter.py

import pytest

from tests.adapter.fixtures.test_class_no_doc import TestClassNoDoc

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
    VrpcAdapter.register(TestClassNoDoc)
    assert VrpcAdapter.get_available_classes() == ["TestClassNoDoc"]


def test_manual_registration_of_a_class_given_a_path():
    # pytest automatically makes the path relative to the root
    VrpcAdapter.register("tests/adapter/fixtures/test_class_doc.py")
    assert VrpcAdapter.get_available_classes() == ["TestClassDoc"]


def test_manual_registration_of_an_instance():
    test_instance = TestClassNoDoc(42)
    VrpcAdapter.register_instance(
        test_instance, class_name="TestClassNoDoc", instance="noDoc1"
    )
    assert VrpcAdapter.get_available_instances("TestClassNoDoc") == ["noDoc1"]


# --- Creation / Deletion and Availability Tests ---


def test_creation_should_not_create_instance_of_non_existing_class():
    with pytest.raises(ValueError, match='"DoesNotExist" is not a registered class'):
        VrpcAdapter.create(class_name="DoesNotExist")


def test_creation_with_minimal_parameters():
    VrpcAdapter.register(TestClassNoDoc)
    # In Python, we can use a simple list to "spy" on events
    create_events = []
    VrpcAdapter._emitter.on("create", lambda data: create_events.append(data))

    instance = VrpcAdapter.create(class_name="TestClassNoDoc")

    assert instance.get_value() == 0
    # One instance was created automatically
    assert len(VrpcAdapter.get_available_instances("TestClassNoDoc")) == 1

    # Check event emission
    assert len(create_events) == 1
    event_data = create_events[0]
    assert event_data["className"] == "TestClassNoDoc"
    assert not event_data["isIsolated"]


def test_creation_with_specific_instance_name_and_args():
    VrpcAdapter.register(TestClassNoDoc)
    instance = VrpcAdapter.create(
        class_name="TestClassNoDoc", instance="myInstance1", args=[42]
    )
    assert instance.get_value() == 42
    assert VrpcAdapter.get_available_instances("TestClassNoDoc") == ["myInstance1"]


def test_creation_in_isolated_mode():
    VrpcAdapter.register(TestClassNoDoc)
    VrpcAdapter.create(class_name="TestClassNoDoc", instance="myInstance1", args=[42])
    VrpcAdapter.create(
        class_name="TestClassNoDoc",
        instance="isolatedInstance",
        args=[-1],
        is_isolated=True,
    )
    # Isolated instances should not appear in the public list
    assert VrpcAdapter.get_available_instances("TestClassNoDoc") == ["myInstance1"]
    # But the instance should exist internally
    assert len(VrpcAdapter._instances) == 2


def test_deletion_of_an_instance():
    VrpcAdapter.register(TestClassNoDoc)
    VrpcAdapter.create(class_name="TestClassNoDoc", instance="instance-to-delete")
    assert len(VrpcAdapter._instances) == 1

    # Test deletion
    was_deleted = VrpcAdapter.delete("instance-to-delete")
    assert was_deleted
    assert len(VrpcAdapter._instances) == 0

    # Test deleting a non-existent instance
    was_not_deleted = VrpcAdapter.delete("does-not-exist")
    assert not was_not_deleted


# --- Documentation Parsing Tests ---


def test_documentation_parsing():
    VrpcAdapter.register("tests/adapter/fixtures/test_class_doc.py")
    meta = VrpcAdapter.get_meta_data("TestClassDoc")

    # Check that the correct function keys were parsed
    assert list(meta.keys()) == ["__createShared__", "get_value", "set_value"]

    # Check constructor (__createShared__) metadata
    constructor_meta = meta["__createShared__"]
    assert constructor_meta["description"] == "Initializes the TestClassDoc."
    assert len(constructor_meta["params"]) == 1
    assert constructor_meta["params"][0]["arg_name"] == "value"
    assert constructor_meta["params"][0]["type_name"] == "int"
    assert (
        constructor_meta["params"][0]["description"] == "Initial value. Defaults to 0."
    )
    assert constructor_meta["params"][0]["default"] == "0"

    # Check setValue metadata
    set_value_meta = meta["set_value"]
    assert set_value_meta["description"] == "Sets a value."
    assert len(set_value_meta["params"]) == 1
    assert set_value_meta["params"][0]["arg_name"] == "value"
    assert set_value_meta["returns"]["type_name"] == "int"
    assert set_value_meta["returns"]["description"] == "The updated value."
