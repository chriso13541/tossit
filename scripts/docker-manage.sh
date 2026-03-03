#!/bin/bash
# docker-manage.sh - TossIt Docker Management Script

set -euo pipefail

COMPOSE_FILE="docker-compose.yml"
IMAGE_NAME="tossit:latest"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() { echo -e "${GREEN}[✓]${NC} $1"; }
print_error() { echo -e "${RED}[✗]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[!]${NC} $1"; }
print_info() { echo -e "${BLUE}[i]${NC} $1"; }

show_help() {
    cat << EOF
TossIt Docker Management Script

Usage: ./docker-manage.sh [COMMAND]

Commands:
  setup         Initial setup and build
  start         Start the cluster
  stop          Stop the cluster
  restart       Restart the cluster
  status        Show cluster status
  logs          View logs (all services)
  logs-brain    View brain server logs
  logs-node     View node logs (specify node number)
  scale         Scale storage nodes
  backup        Backup all data
  restore       Restore from backup
  clean         Clean up (keeps data)
  clean-all     Clean up everything (⚠️  deletes data)
  shell         Open shell in container
  db            Show database info
  stats         Show resource usage
  health        Run health checks
  upgrade       Update and rebuild

Examples:
  ./docker-manage.sh setup
  ./docker-manage.sh start
  ./docker-manage.sh logs-node 1
  ./docker-manage.sh scale 5
  ./docker-manage.sh backup

EOF
}

check_prerequisites() {
    print_info "Checking prerequisites..."
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        print_error "Docker Compose is not installed"
        exit 1
    fi
    
    print_status "Prerequisites OK"
}

check_source_files() {
    local required_files=(
        "brain_server.py"
        "node_agent.py"
        "models.py"
        "tossit_settings.py"
        "tossit_debug_logger.py"
        "tossit_rebalancer.py"
    )
    
    print_info "Checking source files..."
    local missing=0
    
    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            print_error "Missing: $file"
            missing=1
        fi
    done
    
    if [ $missing -eq 1 ]; then
        print_error "Please copy all TossIt source files to this directory"
        exit 1
    fi
    
    print_status "All source files present"
}

setup() {
    print_info "Setting up TossIt Docker environment..."
    
    check_prerequisites
    check_source_files
    
    # Create directories
    print_info "Creating directories..."
    mkdir -p data/{brain,node1,node2,node3}
    mkdir -p logs/{brain,node1,node2,node3}
    mkdir -p debug_logs/{brain,node1,node2,node3}
    mkdir -p backups
    
    # Build image
    print_info "Building Docker image..."
    docker build -t $IMAGE_NAME .
    
    print_status "Setup complete!"
    print_info "Run './docker-manage.sh start' to start the cluster"
}

start_cluster() {
    print_info "Starting TossIt cluster..."
    
    if [ ! -f "Dockerfile" ]; then
        print_error "Dockerfile not found. Run './docker-manage.sh setup' first"
        exit 1
    fi
    
    docker-compose up -d
    
    print_status "Cluster started!"
    print_info "Dashboard: http://localhost:8000"
    print_info "Check status: ./docker-manage.sh status"
}

stop_cluster() {
    print_info "Stopping TossIt cluster..."
    docker-compose down
    print_status "Cluster stopped"
}

restart_cluster() {
    print_info "Restarting TossIt cluster..."
    docker-compose restart
    print_status "Cluster restarted"
}

show_status() {
    print_info "TossIt Cluster Status:"
    echo ""
    docker-compose ps
    echo ""
    
    # Check brain health
    if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
        print_status "Brain server: HEALTHY"
        
        # Get cluster info
        print_info "Cluster nodes:"
        curl -s http://localhost:8000/api/nodes | python3 -m json.tool 2>/dev/null || echo "Could not fetch node info"
    else
        print_error "Brain server: NOT RESPONDING"
    fi
}

view_logs() {
    local service=$1
    
    if [ -z "$service" ]; then
        print_info "Showing logs for all services (Ctrl+C to stop)..."
        docker-compose logs -f
    else
        print_info "Showing logs for $service (Ctrl+C to stop)..."
        docker-compose logs -f "$service"
    fi
}

view_brain_logs() {
    view_logs "tossit-brain"
}

view_node_logs() {
    local node_num=${1:-1}
    view_logs "tossit-node${node_num}"
}

scale_nodes() {
    local count=${1:-3}
    
    print_info "Scaling to $count storage nodes..."
    
    # This requires editing docker-compose.yml to support scaling
    # For now, show instructions
    print_warning "To scale nodes, edit docker-compose.yml and add more node services"
    print_info "Copy the tossit-node1 service and increment the numbers"
}

backup_data() {
    local backup_dir="backups/backup-$(date +%Y%m%d-%H%M%S)"
    
    print_info "Creating backup in $backup_dir..."
    mkdir -p "$backup_dir"
    
    # Backup brain data
    print_info "Backing up brain data..."
    docker exec tossit-brain tar -czf /tmp/brain-backup.tar.gz /app/data 2>/dev/null || true
    docker cp tossit-brain:/tmp/brain-backup.tar.gz "$backup_dir/" 2>/dev/null || true
    
    # Backup each node
    for node in tossit-node1 tossit-node2 tossit-node3; do
        if docker ps --filter "name=$node" --filter "status=running" | grep -q "$node"; then
            print_info "Backing up $node..."
            docker exec "$node" tar -czf /tmp/${node}-backup.tar.gz /app/storage 2>/dev/null || true
            docker cp "$node:/tmp/${node}-backup.tar.gz" "$backup_dir/" 2>/dev/null || true
        fi
    done
    
    print_status "Backup complete: $backup_dir"
}

restore_data() {
    local backup_dir=${1:-}
    
    if [ -z "$backup_dir" ]; then
        print_error "Please specify backup directory"
        print_info "Available backups:"
        ls -lt backups/ | grep "^d" | awk '{print "  " $9}'
        exit 1
    fi
    
    if [ ! -d "$backup_dir" ]; then
        print_error "Backup directory not found: $backup_dir"
        exit 1
    fi
    
    print_warning "This will restore data from: $backup_dir"
    read -p "Continue? (y/N): " -n 1 -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Restore cancelled"
        exit 0
    fi
    
    print_info "Restoring data..."
    
    # Restore brain
    if [ -f "$backup_dir/brain-backup.tar.gz" ]; then
        print_info "Restoring brain data..."
        docker cp "$backup_dir/brain-backup.tar.gz" tossit-brain:/tmp/
        docker exec tossit-brain tar -xzf /tmp/brain-backup.tar.gz -C /
    fi
    
    # Restore nodes
    for node in tossit-node1 tossit-node2 tossit-node3; do
        if [ -f "$backup_dir/${node}-backup.tar.gz" ]; then
            print_info "Restoring $node..."
            docker cp "$backup_dir/${node}-backup.tar.gz" "$node:/tmp/"
            docker exec "$node" tar -xzf "/tmp/${node}-backup.tar.gz" -C /
        fi
    done
    
    print_status "Restore complete"
    print_info "Restart cluster: ./docker-manage.sh restart"
}

clean_cluster() {
    print_warning "This will stop and remove containers (keeps data)"
    read -p "Continue? (y/N): " -n 1 -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Cancelled"
        exit 0
    fi
    
    print_info "Cleaning up containers..."
    docker-compose down
    
    print_status "Cleanup complete (data preserved in data/ and logs/)"
}

clean_all() {
    print_error "⚠️  WARNING: This will DELETE ALL DATA ⚠️"
    print_warning "This includes:"
    print_warning "  - All containers"
    print_warning "  - All stored files"
    print_warning "  - All databases"
    print_warning "  - All logs"
    echo ""
    read -p "Type 'DELETE' to confirm: " confirm
    
    if [ "$confirm" != "DELETE" ]; then
        print_info "Cancelled"
        exit 0
    fi
    
    print_info "Removing everything..."
    docker-compose down -v
    rm -rf data/ logs/ debug_logs/
    
    print_status "All data deleted"
}

open_shell() {
    local container=${1:-tossit-brain}
    
    if ! docker ps --filter "name=$container" | grep -q "$container"; then
        print_error "Container $container is not running"
        print_info "Available containers:"
        docker ps --filter "name=tossit" --format "  {{.Names}}"
        exit 1
    fi
    
    print_info "Opening shell in $container..."
    docker exec -it "$container" /bin/bash
}

show_db_info() {
    print_info "Brain Database Information:"
    echo ""
    
    if ! docker ps --filter "name=tossit-brain" --filter "status=running" | grep -q "tossit-brain"; then
        print_error "Brain server is not running"
        exit 1
    fi
    
    # Get database stats
    docker exec tossit-brain python3 << 'PYTHON'
from models import *
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///data/tossit_brain.db')
Session = sessionmaker(bind=engine)
db = Session()

print("Nodes:", db.query(Node).count())
print("Files:", db.query(File).count())
print("Chunks:", db.query(Chunk).count())
print("Replicas:", db.query(Replica).count())
print("Jobs (Pending):", db.query(Job).filter_by(status=JobStatus.PENDING).count())
print("Jobs (Completed):", db.query(Job).filter_by(status=JobStatus.COMPLETED).count())

db.close()
PYTHON
}

show_stats() {
    print_info "Resource Usage:"
    echo ""
    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}"
    
    echo ""
    print_info "Disk Usage:"
    docker system df
    
    echo ""
    print_info "Storage Usage:"
    for container in tossit-brain tossit-node1 tossit-node2 tossit-node3; do
        if docker ps --filter "name=$container" --filter "status=running" | grep -q "$container"; then
            size=$(docker exec "$container" du -sh /app/storage 2>/dev/null | awk '{print $1}')
            echo "  $container: $size"
        fi
    done
}

health_check() {
    print_info "Running health checks..."
    echo ""
    
    local failed=0
    
    # Check brain
    print_info "Checking brain server..."
    if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
        print_status "Brain: HEALTHY"
    else
        print_error "Brain: FAILED"
        failed=1
    fi
    
    # Check nodes
    for port in 8081 8082 8083; do
        print_info "Checking node on port $port..."
        if curl -sf "http://localhost:$port/health" > /dev/null 2>&1; then
            print_status "Node :$port: HEALTHY"
        else
            print_warning "Node :$port: NOT RESPONDING"
        fi
    done
    
    # Check Docker
    print_info "Checking Docker containers..."
    if docker-compose ps | grep -q "Up"; then
        print_status "Containers: RUNNING"
    else
        print_error "Containers: ISSUE DETECTED"
        failed=1
    fi
    
    echo ""
    if [ $failed -eq 0 ]; then
        print_status "All health checks passed!"
    else
        print_error "Some health checks failed"
        return 1
    fi
}

upgrade_cluster() {
    print_info "Upgrading TossIt cluster..."
    
    # Backup first
    print_info "Creating safety backup..."
    backup_data
    
    # Pull latest code (if in git repo)
    if [ -d ".git" ]; then
        print_info "Pulling latest code..."
        git pull
    else
        print_warning "Not a git repository, skipping code update"
    fi
    
    # Rebuild image
    print_info "Rebuilding Docker image..."
    docker-compose build --no-cache
    
    # Restart services
    print_info "Restarting services..."
    docker-compose down
    docker-compose up -d
    
    print_status "Upgrade complete!"
}

# Main command dispatcher
case "${1:-}" in
    setup)
        setup
        ;;
    start)
        start_cluster
        ;;
    stop)
        stop_cluster
        ;;
    restart)
        restart_cluster
        ;;
    status)
        show_status
        ;;
    logs)
        view_logs "${2:-}"
        ;;
    logs-brain)
        view_brain_logs
        ;;
    logs-node)
        view_node_logs "${2:-1}"
        ;;
    scale)
        scale_nodes "${2:-3}"
        ;;
    backup)
        backup_data
        ;;
    restore)
        restore_data "${2:-}"
        ;;
    clean)
        clean_cluster
        ;;
    clean-all)
        clean_all
        ;;
    shell)
        open_shell "${2:-tossit-brain}"
        ;;
    db)
        show_db_info
        ;;
    stats)
        show_stats
        ;;
    health)
        health_check
        ;;
    upgrade)
        upgrade_cluster
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        print_error "Unknown command: ${1:-}"
        echo ""
        show_help
        exit 1
        ;;
esac