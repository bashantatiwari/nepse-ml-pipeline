FROM python:3.10-slim

WORKDIR /app

# Install system dependencies required for MariaDB C connector
RUN apt-get update && apt-get install -y \
    libmariadb-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Ensure Python treats the working directory as the module root
ENV PYTHONPATH=/app
