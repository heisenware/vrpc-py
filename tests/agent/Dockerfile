# tests/agent/Dockerfile
FROM python:3.12-slim

WORKDIR /usr/src/app

# Copy only the requirements first for better layer caching
COPY tests/agent/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project context
# The 'vrpc' source and the 'tests' directory will be available
COPY ../../ .

# Install the project itself so it's on the python path
RUN pip install -e .
