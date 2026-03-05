FROM python:3.11-slim

WORKDIR /app

# System deps for cryptography + mDNS
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    avahi-daemon \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY backend/*.py ./
COPY frontend/ ../frontend/

# All persistent state lives here — mount a volume to persist, or leave
# unmounted for ephemeral test nodes (data dies with the container).
VOLUME ["/data"]
ENV TOSSIT_DATA_DIR=/data

EXPOSE 8000

CMD ["python", "tossit_node.py"]
