# Use Python 3.10.12 as base image
FROM python:3.10.12-slim

# Install system dependencies required for some Python packages
RUN apt-get update && apt-get install -y \
    build-essential \
    libgdal-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Set Python path
ENV PYTHONPATH=/app
