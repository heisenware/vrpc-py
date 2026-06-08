# VRPC Python

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://img.shields.io/pypi/v/vrpc.svg)](https://pypi.org/project/vrpc/)

**Stop writing API boilerplate.** VRPC (Virtual Remote Procedure Call) allows you to call Python, Node.js, C++, and Arduino classes across any network as if they were local objects. Perfect for microservices, exposing machine learning models, and directly driving React frontends—without the need for REST, GraphQL, or WebSocket boilerplate.

This repository provides both the **Agent** (to expose your Python code) and the **Client** (to seamlessly control remote code from your Python scripts or Jupyter notebooks).

---

## Why VRPC for Python?

- **Zero Boilerplate:** No Flask routers, no Pydantic schemas, and no payload parsing. Just register your standard Python class, and VRPC instantly makes its methods remotely callable.
- **Native Event Proxies:** Don't just fetch data—stream it. VRPC transparently proxies Python callbacks across the network. When your data-science script completes a batch or emits a progress update, your React UI updates instantly.
- **MQTT-Powered NAT Traversal:** Built on top of robust MQTT, agents make outbound connections to your broker. No CORS headaches, no complex reverse proxies, and perfect resilience on unstable networks.
- **Perfect for AI & Data Science:** Expose heavy Pandas processing or PyTorch inference models running on a GPU cluster, and control them directly from a lightweight web server or frontend.

## Installation

Install VRPC via pip:

```bash
pip install vrpc
```

## Quick Start

With VRPC, making a Python class remotely accessible requires almost zero API code.

### 1. Write and Expose your Python Class (Agent)

```python
# backend.py
import time
from vrpc import VrpcAdapter, VrpcAgent

class DataProcessor:
    def __init__(self, dataset_name):
        self.dataset_name = dataset_name
        self.progress = 0

    def process_data(self, factor):
        print(f"Processing {self.dataset_name} with factor {factor}...")
        time.sleep(2) # Simulate heavy work
        return [x * factor for x in range(5)]

    def on_progress(self, callback):
        # VRPC seamlessly proxies this callback over the network!
        for i in range(1, 6):
            time.sleep(0.5)
            self.progress = i * 20
            callback(self.progress)

# 1. Register the class
VrpcAdapter.register(DataProcessor)

# 2. Start the Agent
if __name__ == "__main__":
    agent = VrpcAgent(
        domain="my_domain",
        agent="python_backend",
        broker="mqtts://broker.hivemq.com:8883"
    )
    print("Python Backend is online!")
    agent.serve()
```

### 2. Control it from Anywhere (e.g., Python / Node.js / React)

Once your Python agent is running, you can interact with it transparently from any VRPC client. Here is how you would call it from another Python script:

```python
# client.py
from vrpc import VrpcClient

def run():
    client = VrpcClient(
        domain="my_domain",
        broker="mqtts://broker.hivemq.com:8883"
    )

    # Create a remote instance of your Python class
    processor = client.create(
        agent="python_backend",
        class_name="DataProcessor",
        args=["sales_data_2026"]
    )

    # Listen to remote callbacks across the network!
    def handle_progress(percent):
        print(f"Remote progress: {percent}%")

    processor.on_progress(handle_progress)

    # Call functions natively
    result = processor.process_data(42)
    print(f"Processing complete. Result: {result}")

    client.end()

if __name__ == "__main__":
    run()
```

## The VRPC Ecosystem

Write your performance-critical code in **C++**, your data-science scripts in **Python**, your business logic in **Node.js**, and your IoT firmware on **Arduino**. Call them all identically.

- [VRPC for Node.js / Browser](https://github.com/heisenware/vrpc-js)
- [VRPC for C++](https://github.com/heisenware/vrpc-cpp)
- [VRPC for Arduino / ESP32](https://github.com/heisenware/vrpc-arduino)
- [VRPC for React](https://github.com/heisenware/vrpc-react)

## Documentation

For detailed API references, advanced schema validation, and architecture overviews, please visit our official documentation at **[vrpc.io/docs](https://vrpc.io/docs)**.

## Contributing

Contributions are welcome! Whether it's reporting a bug, proposing a new feature, or submitting a pull request, we'd love your help to make VRPC even better. Please read our [Contributing Guidelines](CONTRIBUTING.md) to get started.

## License

VRPC is released under the [MIT License](LICENSE).
