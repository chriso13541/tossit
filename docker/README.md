# TossIt Docker Deployment

Quick guide to running TossIt with Docker.

## Prerequisites

- Docker 20.10+
- Docker Compose 2.0+

## Quick Start
```bash
# From the docker/ directory
docker-compose up -d

# Access dashboard
open http://localhost:8000
```

## Commands
```bash
# Start cluster
docker-compose up -d

# Stop cluster
docker-compose down

# View logs
docker-compose logs -f

# Check status
docker-compose ps

# Restart
docker-compose restart
```

## Ports

- 8000: Brain server (web dashboard)
- 8081-8083: Storage nodes

## Configuration

Edit environment variables in `docker-compose.yml`:

- `CHUNK_SIZE_MB`: Size of file chunks (default: 64)
- `MIN_REPLICAS`: Minimum copies per chunk (default: 2)
- `STORAGE_PERCENT`: Storage allocation % (default: 75)

See main [documentation](../README.md) for more details.
