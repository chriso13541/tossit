# Multi-stage build for TossIt Distributed Storage
FROM python:3.11-slim as base

# Build argument to determine service type
ARG SERVICE_TYPE=both

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    net-tools \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy TossIt source code
COPY *.py ./

# Create necessary directories
RUN mkdir -p /app/storage /app/logs /app/debug_logs /app/data /app/config

# Copy entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV TOSSIT_MODE=${SERVICE_TYPE}

# Expose ports
# 8000 for brain, 8080 for nodes
EXPOSE 8000 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:${BRAIN_PORT:-8080}/health || exit 1

# Use entrypoint script
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

# Default command (can be overridden)
CMD ["python3"]