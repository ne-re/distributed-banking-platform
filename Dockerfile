# Start from an official, lightweight Python 3.11 Linux image
FROM python:3.11-slim

# Prevent Python from buffering stdout/stderr so logs appear immediately in Docker
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file first (Docker caches this step for fast rebuilds)
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy our application source code into the container
COPY services/ ./services/
COPY client/ ./client/