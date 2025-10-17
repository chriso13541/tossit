#!/bin/bash
set -e

# TossIt Docker Entrypoint Script
echo "=========================================="
echo "TossIt Container Starting"
echo "=========================================="
echo "Mode: ${TOSSIT_MODE}"
echo "Hostname: $(hostname)"
echo "IP Address: $(hostname -i)"
echo ""

# Function to generate config file
generate_config() {
    local mode=$1
    local config_file="/app/config/tossit_config.json"
    
    if [ "$mode" = "brain" ]; then
        cat > "$config_file" << CONFIGEOF
{
  "brain": {
    "ip_address": "0.0.0.0",
    "port": ${BRAIN_PORT:-8000},
    "database_path": "/app/data/tossit_brain.db",
    "chunk_size_mb": ${CHUNK_SIZE_MB:-64},
    "min_replicas": ${MIN_REPLICAS:-2},
    "max_replicas": ${MAX_REPLICAS:-3},
    "enable_web_ui": true
  },
  "cluster": {
    "heartbeat_interval_seconds": 30,
    "job_check_interval_seconds": 5,
    "replication_priority": 7,
    "verification_interval_hours": 24,
    "auto_rebalance_enabled": true,
    "auto_rebalance_threshold": 80,
    "max_migrations_per_rebalance": 50
  }
}
CONFIGEOF
    elif [ "$mode" = "node" ]; then
        cat > "$config_file" << CONFIGEOF
{
  "node": {
    "name": "${NODE_NAME:-$(hostname)}",
    "storage_path": "/app/storage",
    "port": ${NODE_PORT:-8080},
    "brain_url": "${BRAIN_URL:-http://tossit-brain:8000}",
    "storage_mode": "${STORAGE_MODE:-percentage}",
    "storage_limit_percent": ${STORAGE_PERCENT:-75},
    "storage_limit_gb": ${STORAGE_GB:-100},
    "auto_register": true,
    "heartbeat_interval": 30,
    "job_check_interval": 5
  }
}
CONFIGEOF
    else
        echo "Error: Unknown mode '$mode'"
        exit 1
    fi
    
    echo "✓ Generated configuration: $config_file"
}

# Wait for brain to be ready (for nodes)
wait_for_brain() {
    local brain_url="${BRAIN_URL:-http://tossit-brain:8000}"
    local max_attempts=30
    local attempt=1
    
    echo "Waiting for brain server at $brain_url..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s -f "${brain_url}/api/nodes" > /dev/null 2>&1; then
            echo "✓ Brain server is ready!"
            return 0
        fi
        
        echo "  Attempt $attempt/$max_attempts - Brain not ready yet..."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo "✗ Brain server did not become ready in time"
    return 1
}

# Ensure directories exist
mkdir -p /app/storage /app/logs /app/debug_logs /app/data /app/config
chmod -R 755 /app/storage /app/logs /app/debug_logs /app/data /app/config

# Generate configuration based on mode
case "${TOSSIT_MODE}" in
    brain)
        echo "Starting as BRAIN server..."
        generate_config "brain"
        
        # Wait a moment for network to be ready
        sleep 2
        
        # Start brain server
        exec python3 brain_server.py
        ;;
        
    node)
        echo "Starting as STORAGE NODE..."
        generate_config "node"
        
        # Wait for brain to be ready
        if ! wait_for_brain; then
            echo "Warning: Starting anyway, will retry connection..."
        fi
        
        # Start node agent
        exec python3 node_agent.py --config /app/config/tossit_config.json
        ;;
        
    both)
        echo "Starting as COMBINED (brain + node)..."
        
        # Generate brain config
        generate_config "brain"
        
        # Start brain in background
        python3 brain_server.py &
        BRAIN_PID=$!
        
        # Wait for brain to be ready on the actual interface
        sleep 5
        
        # Get the container's IP address
        CONTAINER_IP=$(hostname -i)
        
        # Generate node config with correct brain URL using container IP
        cat > /app/config/node_config.json << CONFIGEOF
{
  "node": {
    "name": "$(hostname)-storage",
    "storage_path": "/app/storage",
    "port": ${NODE_PORT:-8081},
    "brain_url": "http://${CONTAINER_IP}:8000",
    "storage_mode": "${STORAGE_MODE:-percentage}",
    "storage_limit_percent": ${STORAGE_PERCENT:-60},
    "storage_limit_gb": ${STORAGE_GB:-100},
    "auto_register": true
  }
}
CONFIGEOF
        
        # Wait a bit more for brain to be fully ready
        sleep 3
        
        # Start node agent in foreground
        exec python3 node_agent.py --config /app/config/node_config.json
        ;;
        
    *)
        echo "Error: Unknown TOSSIT_MODE: ${TOSSIT_MODE}"
        echo "Valid modes: brain, node, both"
        exit 1
        ;;
esac
