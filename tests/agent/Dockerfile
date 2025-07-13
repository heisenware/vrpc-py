# tests/agent/Dockerfile
FROM python:3.12-slim

WORKDIR /usr/src/app

# Copy only the requirements first for better layer caching
COPY tests/agent/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
