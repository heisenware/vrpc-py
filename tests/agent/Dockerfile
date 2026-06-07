# tests/agent/Dockerfile
FROM python:3.12-slim

WORKDIR /usr/src/app

# Upgrade pip to the latest version
RUN pip install --no-cache-dir --upgrade pip

# Copy the project configuration and source code
COPY pyproject.toml README.md ./
COPY vrpc/ vrpc/

# Install the package and its test dependencies
RUN pip install --no-cache-dir .[test]
