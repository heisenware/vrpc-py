# vrpc

> **Variadic Remote Procedure Calls for Python**

`vrpc` is an asynchronous, event-driven Remote Procedure Call (RPC) framework for Python. It allows you to seamlessly expose standard Python classes and functions over an MQTT message broker, making them instantly callable from anywhere in the world.

By leveraging MQTT and Python's `asyncio`, `vrpc` bypasses NATs, firewalls, and complex networking setups, allowing for bi-directional communication, dynamic object instantiation, and remote continuous event streaming.

It is 100% protocol-compatible with the [Node.js / JS `vrpc` implementation](https://github.com/heisenware/vrpc).

## ✨ Features

- **Zero Boilerplate:** Register existing Python classes without modifying their code.
- **Fully Asynchronous:** Built from the ground up on modern `asyncio` and `aiomqtt`.
- **Stateful Instances:** Create, manage, and delete remote object instances dynamically. Shared instances can be interacted with by multiple clients simultaneously.
- **Remote Event Listeners:** Pass Python callables as arguments to remote functions. VRPC automatically binds them to MQTT streams (perfect for real-time sensor data or status updates).
- **Batch Executions:** Broadcast method calls to all instances of a class across multiple distributed agents in a single line of code (`call_all`).

## 📦 Installation

Since `vrpc` is modern Python package (PEP 621), you can install it directly via pip:

```bash
pip install vrpc
```

_Requirements: Python 3.8+_

## 🚀 Quick Start

To use VRPC, you need two components: an **Agent** (which hosts your code) and a **Client** (which remotely calls it). Both connect to a central MQTT broker.

### 1. The Agent (Server)

First, write a standard Python class. Let's create a `Counter` that maintains state and can emit continuous events via a callback.

```python
# agent.py
import asyncio
from vrpc import VrpcAdapter, VrpcAgent

class Counter:
    def __init__(self, initial_value=0):
        self._count = initial_value
        self._callback = None

    def on_change(self, callback):
        """Registers a callback for continuous state updates."""
        self._callback = callback

    def increment(self, step=1):
        """Increments the counter and triggers the callback."""
        self._count += step
        if self._callback:
            self._callback(self._count)
        return self._count

# 1. Register the class so VRPC knows about it
VrpcAdapter.register(Counter)

async def main():
    # 2. Configure the Agent to connect to a broker
    agent = VrpcAgent(
        domain="my.custom.domain",
        agent="python-agent-1",
        broker="mqtt://broker.hivemq.com:1883" # Public test broker
    )

    print("Agent is starting...")
    await agent.serve()

if __name__ == "__main__":
    asyncio.run(main())
```

### 2. The Client

Now, from a completely different machine, process, or network, we can connect a client, create an instance of that `Counter`, and interact with it.

```python
# client.py
import asyncio
from vrpc import VrpcClient

async def main():
    # 1. Connect to the same broker and domain
    client = VrpcClient(
        domain="my.custom.domain",
        broker="mqtt://broker.hivemq.com:1883"
    )
    await client.connect()

    # 2. Create a remote instance of the Counter
    print("Creating remote Counter instance...")
    counter = await client.create(
        agent="python-agent-1",
        class_name="Counter",
        instance="my-shared-counter",
        args=[10]  # Passes '10' to initial_value
    )

    # 3. Define a local function to handle remote events
    def handle_update(new_val):
        print(f" -> Continuous Event Received! Counter is now: {new_val}")

    # VRPC magically wires this Python function over MQTT
    await counter.on_change(handle_update)

    # 4. Call remote methods
    print("Calling increment(5)...")
    result = await counter.increment(5)
    print(f"RPC Returned: {result}")

    # Cleanup
    await client.end()

if __name__ == "__main__":
    asyncio.run(main())
```

## 🏗 Architecture Concepts

VRPC organizes remote execution using a specific hierarchy:

- **Domain:** The highest level of isolation. Agents and Clients must share the same domain to see each other.
- **Agent:** A physical process hosting VRPC code. A single domain can have hundreds of distributed agents.
- **Class:** A registered Python class available for instantiation.
- **Instance:** A specific, stateful object created from a Class. Instances can be **Shared** (visible to all clients) or **Isolated** (visible only to the client that created it).

## 🤝 Contributing

Contributions, issues, and feature requests are welcome!

1. Fork the project.
2. Install with test dependencies: `pip install -e .[test]`
3. Run local adapter tests: `pytest tests/adapter/test_adapter.py`
4. Run integration tests (requires Docker): `cd tests/agent && ./test.sh`

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
