#!/bin/bash
# Start TossIt Node

cd "$(dirname "$0")"

# Activate venv if it exists
if [ -d "../venv" ]; then
    source ../venv/bin/activate
fi

# Sync to tossit registry
export TOSSIT_REGISTRY_URL="https://tossit.cc"


# Run the node
python3 backend/tossit_node.py
