.PHONY: help setup build start stop restart status logs clean backup

.DEFAULT_GOAL := help

help:
	@echo "TossIt Docker Management"
	@echo ""
	@echo "Available targets:"
	@echo "  setup       - Initial setup and build"
	@echo "  start       - Start the cluster"
	@echo "  stop        - Stop the cluster"
	@echo "  restart     - Restart the cluster"
	@echo "  status      - Show cluster status"
	@echo "  logs        - View logs"
	@echo "  logs-brain  - View brain logs"
	@echo "  logs-node1  - View node1 logs"
	@echo "  clean       - Stop and remove containers"
	@echo "  backup      - Backup data"
	@echo "  health      - Run health checks"
	@echo "  shell       - Open shell in brain"

setup:
	@echo "Setting up TossIt..."
	@mkdir -p data/{brain,node1,node2,node3}
	@mkdir -p logs/{brain,node1,node2,node3}
	@mkdir -p debug_logs backups
	@docker build -t tossit:latest .
	@echo "✓ Setup complete!"

start:
	@docker-compose up -d
	@echo "✓ Cluster started! Dashboard: http://localhost:8000"

stop:
	@docker-compose down

restart:
	@docker-compose restart

status:
	@docker-compose ps

logs:
	@docker-compose logs -f

logs-brain:
	@docker-compose logs -f tossit-brain

logs-node1:
	@docker-compose logs -f tossit-node1

logs-node2:
	@docker-compose logs -f tossit-node2

logs-node3:
	@docker-compose logs -f tossit-node3

clean:
	@docker-compose down

backup:
	@mkdir -p backups/backup-$$(date +%Y%m%d-%H%M%S)
	@echo "Creating backup..."

health:
	@curl -sf http://localhost:8000/health && echo "✓ Brain: HEALTHY" || echo "✗ Brain: FAILED"
	@curl -sf http://localhost:8081/health && echo "✓ Node1: HEALTHY" || echo "✗ Node1: FAILED"
	@curl -sf http://localhost:8082/health && echo "✓ Node2: HEALTHY" || echo "✗ Node2: FAILED"
	@curl -sf http://localhost:8083/health && echo "✓ Node3: HEALTHY" || echo "✗ Node3: FAILED"

shell:
	@docker exec -it tossit-brain /bin/bash

stats:
	@docker stats --no-stream
