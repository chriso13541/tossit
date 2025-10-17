#!/bin/bash

# TossIt Distributed Storage Cluster - Enhanced Installation Script
# Supports interactive setup with auto-detection and validation
# Compatible with Debian, Ubuntu, RHEL, CentOS, Fedora

set -euo pipefail

# ============================================================================
# CONFIGURATION
# ============================================================================

SCRIPT_VERSION="2.0"
MIN_PYTHON_VERSION="3.8"
REQUIRED_DISK_SPACE_GB=5

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

print_banner() {
    echo -e "${BLUE}"
    cat << "EOF"
╔════════════════════════════════════════════════════════════════╗
║                                                                ║
║   ████████╗ ██████╗ ███████╗███████╗██╗████████╗             ║
║   ╚══██╔══╝██╔═══██╗██╔════╝██╔════╝██║╚══██╔══╝             ║
║      ██║   ██║   ██║███████╗███████╗██║   ██║                ║
║      ██║   ██║   ██║╚════██║╚════██║██║   ██║                ║
║      ██║   ╚██████╔╝███████║███████║██║   ██║                ║
║      ╚═╝    ╚═════╝ ╚══════╝╚══════╝╚═╝   ╚═╝                ║
║                                                                ║
║            Distributed Storage Cluster Installer              ║
║                       Version 2.0                              ║
╚════════════════════════════════════════════════════════════════╝
EOF
    echo -e "${NC}"
}

print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

print_header() {
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════${NC}"
}

print_info() {
    echo -e "${BLUE}[i]${NC} $1"
}

prompt_user() {
    local prompt="$1"
    local default="$2"
    local response
    
    if [ -n "$default" ]; then
        echo -ne "${MAGENTA}[?]${NC} $prompt ${CYAN}[$default]${NC}: "
    else
        echo -ne "${MAGENTA}[?]${NC} $prompt: "
    fi
    
    read -r response
    echo "${response:-$default}"
}

prompt_yes_no() {
    local prompt="$1"
    local default="${2:-y}"
    local response
    
    if [ "$default" = "y" ]; then
        echo -ne "${MAGENTA}[?]${NC} $prompt ${CYAN}[Y/n]${NC}: "
    else
        echo -ne "${MAGENTA}[?]${NC} $prompt ${CYAN}[y/N]${NC}: "
    fi
    
    read -r response
    response="${response:-$default}"
    
    [[ "$response" =~ ^[Yy] ]]
}

check_command() {
    command -v "$1" >/dev/null 2>&1
}

get_os_info() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS_NAME="$ID"
        OS_VERSION="$VERSION_ID"
    elif check_command lsb_release; then
        OS_NAME=$(lsb_release -si | tr '[:upper:]' '[:lower:]')
        OS_VERSION=$(lsb_release -sr)
    else
        OS_NAME="unknown"
        OS_VERSION="unknown"
    fi
}

detect_package_manager() {
    if check_command apt-get; then
        PKG_MANAGER="apt"
        PKG_UPDATE="apt-get update"
        PKG_INSTALL="apt-get install -y"
    elif check_command dnf; then
        PKG_MANAGER="dnf"
        PKG_UPDATE="dnf check-update || true"
        PKG_INSTALL="dnf install -y"
    elif check_command yum; then
        PKG_MANAGER="yum"
        PKG_UPDATE="yum check-update || true"
        PKG_INSTALL="yum install -y"
    else
        PKG_MANAGER="unknown"
    fi
}

get_local_ip() {
    local ip
    
    # Try multiple methods to get IP
    if check_command ip; then
        ip=$(ip route get 8.8.8.8 2>/dev/null | grep -oP 'src \K\S+')
    fi
    
    if [ -z "$ip" ] && check_command hostname; then
        ip=$(hostname -I 2>/dev/null | awk '{print $1}')
    fi
    
    if [ -z "$ip" ]; then
        ip="127.0.0.1"
    fi
    
    echo "$ip"
}

check_disk_space() {
    local path="$1"
    local required_gb="$2"
    
    local available_kb=$(df "$path" | awk 'NR==2 {print $4}')
    local available_gb=$((available_kb / 1024 / 1024))
    
    if [ "$available_gb" -lt "$required_gb" ]; then
        print_error "Insufficient disk space. Required: ${required_gb}GB, Available: ${available_gb}GB"
        return 1
    fi
    
    print_status "Sufficient disk space: ${available_gb}GB available"
    return 0
}

verify_python_version() {
    local version=$1
    python3 -c "import sys; exit(0 if sys.version_info >= tuple(map(int, '$version'.split('.'))) else 1)"
}

# ============================================================================
# PRE-FLIGHT CHECKS
# ============================================================================

run_preflight_checks() {
    print_header "Pre-flight System Checks"
    
    # Check if running as root
    if [[ $EUID -eq 0 ]]; then
        print_error "Please run as a regular user, not as root"
        print_info "The script will request sudo access when needed"
        exit 1
    fi
    
    # Check sudo access
    print_info "Checking sudo access..."
    if ! sudo -n true 2>/dev/null; then
        print_warning "This script requires sudo access for system packages"
        if ! sudo -v; then
            print_error "Failed to obtain sudo access"
            exit 1
        fi
    fi
    print_status "Sudo access confirmed"
    
    # Detect OS
    print_info "Detecting operating system..."
    get_os_info
    detect_package_manager
    print_status "OS: $OS_NAME $OS_VERSION"
    print_status "Package Manager: $PKG_MANAGER"
    
    # Check Python
    print_info "Checking Python installation..."
    if ! check_command python3; then
        print_error "Python 3 is not installed"
        print_info "Install with: sudo $PKG_INSTALL python3"
        exit 1
    fi
    
    PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
    print_status "Found Python $PYTHON_VERSION"
    
    if ! verify_python_version "$MIN_PYTHON_VERSION"; then
        print_error "Python $MIN_PYTHON_VERSION or higher is required"
        exit 1
    fi
    print_status "Python version is compatible"
    
    # Check disk space
    print_info "Checking available disk space..."
    if ! check_disk_space "." "$REQUIRED_DISK_SPACE_GB"; then
        exit 1
    fi
    
    print_status "All pre-flight checks passed!"
}

# ============================================================================
# SYSTEM DEPENDENCIES
# ============================================================================

install_system_dependencies() {
    print_header "Installing System Dependencies"
    
    if [ "$PKG_MANAGER" = "unknown" ]; then
        print_warning "Unknown package manager. Skipping system package installation."
        print_warning "Please ensure you have: python3, python3-pip, python3-venv, build tools"
        return 0
    fi
    
    print_info "Updating package lists..."
    if ! sudo $PKG_UPDATE; then
        print_warning "Failed to update package lists (non-fatal)"
    fi
    
    print_info "Installing required packages..."
    
    case $PKG_MANAGER in
        apt)
            PYTHON_VERSION_SHORT=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
            sudo $PKG_INSTALL \
                python3 \
                python3-pip \
                python3-venv \
                python${PYTHON_VERSION_SHORT}-venv \
                python3-dev \
                build-essential \
                curl \
                git \
                net-tools || {
                print_error "Failed to install some packages"
                print_info "Try running manually: sudo $PKG_INSTALL python3 python3-pip python3-venv"
                exit 1
            }
            ;;
        dnf|yum)
            sudo $PKG_INSTALL \
                python3 \
                python3-pip \
                python3-devel \
                gcc \
                gcc-c++ \
                make \
                curl \
                git \
                net-tools || {
                print_error "Failed to install some packages"
                exit 1
            }
            ;;
    esac
    
    print_status "System dependencies installed"
}

# ============================================================================
# PYTHON ENVIRONMENT
# ============================================================================

setup_python_environment() {
    print_header "Setting Up Python Virtual Environment"
    
    # Test venv module
    print_info "Testing virtual environment support..."
    if ! python3 -m venv --help >/dev/null 2>&1; then
        print_error "Python venv module not available"
        print_info "Install with: sudo $PKG_INSTALL python3-venv"
        exit 1
    fi
    print_status "Virtual environment support confirmed"
    
    # Create virtual environment
    if [ -d "venv" ]; then
        print_warning "Virtual environment already exists"
        if prompt_yes_no "Recreate virtual environment?" "n"; then
            rm -rf venv
        else
            print_info "Using existing virtual environment"
            return 0
        fi
    fi
    
    print_info "Creating virtual environment..."
    if ! python3 -m venv venv; then
        print_error "Failed to create virtual environment"
        exit 1
    fi
    print_status "Virtual environment created"
    
    # Activate and verify
    print_info "Activating virtual environment..."
    source venv/bin/activate
    
    if [ -z "$VIRTUAL_ENV" ]; then
        print_error "Virtual environment activation failed"
        exit 1
    fi
    print_status "Virtual environment activated: $VIRTUAL_ENV"
    
    # Upgrade pip
    print_info "Upgrading pip..."
    if ! pip install --upgrade pip setuptools wheel --quiet; then
        print_warning "Failed to upgrade pip (continuing anyway)"
    fi
    print_status "pip upgraded"
    
    # Install dependencies
    print_info "Installing TossIt Python dependencies..."
    
    cat > requirements.txt << 'EOF'
fastapi==0.104.1
uvicorn[standard]==0.24.0
aiohttp==3.9.0
sqlalchemy==2.0.23
psutil==5.9.6
python-multipart==0.0.6
aiofiles==24.1.0
pydantic>=2.9.0
EOF
    
    if ! pip install -r requirements.txt; then
        print_error "Failed to install Python dependencies"
        print_info "Try manually: source venv/bin/activate && pip install -r requirements.txt"
        exit 1
    fi
    
    # Verify installations
    print_info "Verifying package installations..."
    if python3 -c "import fastapi, uvicorn, aiohttp, sqlalchemy, psutil" 2>/dev/null; then
        print_status "All required packages installed and verified"
    else
        print_error "Package verification failed"
        exit 1
    fi
}

# ============================================================================
# INTERACTIVE CONFIGURATION
# ============================================================================

collect_configuration() {
    print_header "TossIt Configuration Setup"
    
    print_info "This wizard will help you configure TossIt for your environment."
    echo ""
    
    # Installation mode
    print_info "Installation modes:"
    echo "  1) Brain Server (central coordinator)"
    echo "  2) Storage Node (distributed storage)"
    echo "  3) Both (brain + storage on same machine)"
    echo ""
    
    local mode
    mode=$(prompt_user "Select installation mode [1-3]" "3")
    
    case $mode in
        1) INSTALL_MODE="brain" ;;
        2) INSTALL_MODE="node" ;;
        3) INSTALL_MODE="both" ;;
        *) INSTALL_MODE="both" ;;
    esac
    
    # Network configuration
    print_header "Network Configuration"
    
    LOCAL_IP=$(get_local_ip)
    print_info "Auto-detected IP address: $LOCAL_IP"
    
    if ! prompt_yes_no "Use this IP address?" "y"; then
        LOCAL_IP=$(prompt_user "Enter IP address" "$LOCAL_IP")
    fi
    
    # Brain configuration
    if [[ "$INSTALL_MODE" == "brain" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        print_header "Brain Server Configuration"
        echo ""  # Add blank line
        
        echo -ne "${MAGENTA}[?]${NC} Brain server port ${CYAN}[8000]${NC}: "
        read -r BRAIN_PORT
        BRAIN_PORT="${BRAIN_PORT:-8000}"
        
        echo -ne "${MAGENTA}[?]${NC} Default chunk size (MB) ${CYAN}[64]${NC}: "
        read -r CHUNK_SIZE
        CHUNK_SIZE="${CHUNK_SIZE:-64}"
        
        echo -ne "${MAGENTA}[?]${NC} Minimum replicas per chunk ${CYAN}[2]${NC}: "
        read -r MIN_REPLICAS
        MIN_REPLICAS="${MIN_REPLICAS:-2}"
        
        echo -ne "${MAGENTA}[?]${NC} Maximum replicas per chunk ${CYAN}[3]${NC}: "
        read -r MAX_REPLICAS
        MAX_REPLICAS="${MAX_REPLICAS:-3}"
    fi
    
    # Node configuration
    if [[ "$INSTALL_MODE" == "node" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        print_header "Storage Node Configuration"
        echo ""  # Add blank line
        
        if [[ "$INSTALL_MODE" == "both" ]]; then
            DEFAULT_NODE_NAME="$(hostname)-brain"
            echo -ne "${MAGENTA}[?]${NC} Node name ${CYAN}[$DEFAULT_NODE_NAME]${NC}: "
            read -r NODE_NAME
            NODE_NAME="${NODE_NAME:-$DEFAULT_NODE_NAME}"
            
            BRAIN_URL="http://${LOCAL_IP}:${BRAIN_PORT}"
            
            echo -ne "${MAGENTA}[?]${NC} Node port ${CYAN}[8081]${NC}: "
            read -r NODE_PORT
            NODE_PORT="${NODE_PORT:-8081}"
        else
            DEFAULT_NODE_NAME="$(hostname)"
            echo -ne "${MAGENTA}[?]${NC} Node name ${CYAN}[$DEFAULT_NODE_NAME]${NC}: "
            read -r NODE_NAME
            NODE_NAME="${NODE_NAME:-$DEFAULT_NODE_NAME}"
            
            echo -ne "${MAGENTA}[?]${NC} Brain server URL ${CYAN}[http://192.168.1.100:8000]${NC}: "
            read -r BRAIN_URL
            BRAIN_URL="${BRAIN_URL:-http://192.168.1.100:8000}"
            
            echo -ne "${MAGENTA}[?]${NC} Node port ${CYAN}[8080]${NC}: "
            read -r NODE_PORT
            NODE_PORT="${NODE_PORT:-8080}"
        fi
        
        # Storage configuration
        echo ""
        print_info "Storage allocation modes:"
        echo "  1) Percentage - Use a percentage of available space"
        echo "  2) Fixed GB - Use a fixed amount of space"
        echo "  3) Full - Use all available space (not recommended)"
        echo ""
        
        echo -ne "${MAGENTA}[?]${NC} Select storage mode [1-3] ${CYAN}[1]${NC}: "
        read -r storage_mode
        storage_mode="${storage_mode:-1}"
        
        case $storage_mode in
            1) 
                STORAGE_MODE="percentage"
                echo -ne "${MAGENTA}[?]${NC} Percentage of disk to use ${CYAN}[50]${NC}: "
                read -r STORAGE_PERCENT
                STORAGE_PERCENT="${STORAGE_PERCENT:-50}"
                ;;
            2) 
                STORAGE_MODE="fixed_gb"
                echo -ne "${MAGENTA}[?]${NC} Fixed storage amount (GB) ${CYAN}[100]${NC}: "
                read -r STORAGE_GB
                STORAGE_GB="${STORAGE_GB:-100}"
                ;;
            3) 
                STORAGE_MODE="full"
                print_warning "Full mode will use ALL available space - use with caution!"
                ;;
            *)
                STORAGE_MODE="percentage"
                STORAGE_PERCENT="50"
                ;;
        esac
    fi
    
    # Confirmation
    print_header "Configuration Summary"
    echo ""
    echo "Installation Mode: $INSTALL_MODE"
    echo "Local IP: $LOCAL_IP"
    
    if [[ "$INSTALL_MODE" == "brain" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        echo ""
        echo "Brain Server:"
        echo "  Port: $BRAIN_PORT"
        echo "  Chunk Size: ${CHUNK_SIZE}MB"
        echo "  Replicas: ${MIN_REPLICAS}-${MAX_REPLICAS}"
    fi
    
    if [[ "$INSTALL_MODE" == "node" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        echo ""
        echo "Storage Node:"
        echo "  Name: $NODE_NAME"
        echo "  Port: $NODE_PORT"
        echo "  Brain URL: $BRAIN_URL"
        echo "  Storage Mode: $STORAGE_MODE"
        if [ "$STORAGE_MODE" = "percentage" ]; then
            echo "  Storage Allocation: ${STORAGE_PERCENT}%"
        elif [ "$STORAGE_MODE" = "fixed_gb" ]; then
            echo "  Storage Allocation: ${STORAGE_GB}GB"
        fi
    fi
    
    echo ""
    if ! prompt_yes_no "Proceed with this configuration?" "y"; then
        print_info "Configuration cancelled. Please run the installer again."
        exit 0
    fi
}

# ============================================================================
# DIRECTORY STRUCTURE
# ============================================================================

create_directory_structure() {
    print_header "Creating Directory Structure"
    
    local dirs=("storage" "logs" "debug_logs" "backups" "config")
    
    for dir in "${dirs[@]}"; do
        if [ ! -d "$dir" ]; then
            mkdir -p "$dir"
            print_status "Created $dir/"
        else
            print_info "Directory $dir/ already exists"
        fi
    done
    
    print_status "Directory structure ready"
}

# ============================================================================
# CONFIGURATION FILES
# ============================================================================

generate_config_files() {
    print_header "Generating Configuration Files"
    
    # Brain configuration
    if [[ "$INSTALL_MODE" == "brain" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        print_info "Creating brain_config.json..."
        
        cat > config/brain_config.json << EOF
{
  "brain": {
    "ip_address": "${LOCAL_IP}",
    "port": ${BRAIN_PORT},
    "database_path": "./tossit_brain.db",
    "chunk_size_mb": ${CHUNK_SIZE},
    "min_replicas": ${MIN_REPLICAS},
    "max_replicas": ${MAX_REPLICAS},
    "enable_web_ui": true
  },
  "cluster": {
    "heartbeat_interval_seconds": 30,
    "job_check_interval_seconds": 5,
    "replication_priority": 7,
    "verification_interval_hours": 24,
    "auto_rebalance_threshold": 80,
    "max_migrations_per_rebalance": 50
  }
}
EOF
        print_status "Created config/brain_config.json"
    fi
    
    # Node configuration
    if [[ "$INSTALL_MODE" == "node" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        print_info "Creating node configuration..."
        
        local config_file="config/${NODE_NAME}_config.json"
        
        cat > "$config_file" << EOF
{
  "node": {
    "name": "${NODE_NAME}",
    "storage_path": "./storage",
    "port": ${NODE_PORT},
    "brain_url": "${BRAIN_URL}",
    "storage_mode": "${STORAGE_MODE}",
EOF

        if [ "$STORAGE_MODE" = "percentage" ]; then
            cat >> "$config_file" << EOF
    "storage_limit_percent": ${STORAGE_PERCENT},
EOF
        elif [ "$STORAGE_MODE" = "fixed_gb" ]; then
            cat >> "$config_file" << EOF
    "storage_limit_gb": ${STORAGE_GB},
EOF
        fi

        cat >> "$config_file" << EOF
    "auto_register": true,
    "heartbeat_interval": 30,
    "job_check_interval": 5
  }
}
EOF
        print_status "Created $config_file"
    fi
    
    print_status "Configuration files generated"
}

# ============================================================================
# START SCRIPTS
# ============================================================================

create_start_scripts() {
    print_header "Creating Management Scripts"
    
    # Brain start script
    if [[ "$INSTALL_MODE" == "brain" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        cat > start_brain.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")"

if [[ ! -f "venv/bin/activate" ]]; then
    echo "Error: Virtual environment not found."
    echo "Run the installer first: ./improved_installer.sh"
    exit 1
fi

source venv/bin/activate

if [[ ! -f "brain_server.py" ]]; then
    echo "Error: brain_server.py not found"
    echo ""
    echo "Please copy your TossIt source files to this directory:"
    echo "  - brain_server.py"
    echo "  - models.py"
    echo "  - tossit_settings.py"
    echo "  - tossit_debug_logger.py"
    echo "  - tossit_rebalancer.py"
    exit 1
fi

echo "Starting TossIt Brain Server..."
echo "Dashboard: http://0.0.0.0:8000"
echo "Press Ctrl+C to stop"
echo ""

python3 brain_server.py
EOF
        chmod +x start_brain.sh
        print_status "Created start_brain.sh"
    fi
    
    # Node start script
    if [[ "$INSTALL_MODE" == "node" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        cat > start_node.sh << EOF
#!/bin/bash
cd "\$(dirname "\$0")"

if [[ ! -f "venv/bin/activate" ]]; then
    echo "Error: Virtual environment not found."
    exit 1
fi

source venv/bin/activate

if [[ ! -f "node_agent.py" ]]; then
    echo "Error: node_agent.py not found"
    exit 1
fi

CONFIG_FILE="config/${NODE_NAME}_config.json"

if [ ! -f "\$CONFIG_FILE" ]; then
    echo "Error: Configuration file not found: \$CONFIG_FILE"
    exit 1
fi

echo "Starting TossIt Node: ${NODE_NAME}"
echo "Using config: \$CONFIG_FILE"
echo "Press Ctrl+C to stop"
echo ""

python3 node_agent.py --config "\$CONFIG_FILE"
EOF
        chmod +x start_node.sh
        print_status "Created start_node.sh"
    fi
    
    # Status check script
    cat > check_status.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")"

echo "TossIt Cluster Status"
echo "===================="
echo ""

# Check brain server
if curl -s --connect-timeout 5 http://localhost:8000/health > /dev/null 2>&1; then
    echo "✓ Brain server: RUNNING (http://localhost:8000)"
else
    echo "✗ Brain server: NOT RUNNING"
fi

echo ""
echo "Active Processes:"
ps aux | grep -E "(brain_server|node_agent)" | grep -v grep || echo "  No TossIt processes found"

echo ""
echo "Virtual Environment:"
if [[ -f "venv/bin/activate" ]]; then
    echo "  ✓ Virtual environment exists"
else
    echo "  ✗ Virtual environment missing"
fi
EOF
    chmod +x check_status.sh
    print_status "Created check_status.sh"
    
    # System info script
    cat > system_info.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")"

echo "TossIt System Information"
echo "========================"
echo ""

echo "Python:"
echo "  Version: $(python3 --version 2>&1)"
echo "  Location: $(which python3)"
echo ""

echo "System:"
echo "  OS: $(uname -s)"
echo "  Kernel: $(uname -r)"
echo "  Architecture: $(uname -m)"
echo "  Hostname: $(hostname)"
echo ""

echo "Network:"
LOCAL_IP=$(ip route get 8.8.8.8 2>/dev/null | grep -oP 'src \K\S+' || hostname -I | awk '{print $1}')
echo "  Local IP: $LOCAL_IP"
echo ""

echo "Storage:"
echo "  Installation: $(pwd)"
df -h . | awk 'NR==2 {print "  Available: "$4}'
du -sh storage 2>/dev/null | awk '{print "  TossIt Usage: "$1}' || echo "  TossIt Usage: 0B"
EOF
    chmod +x system_info.sh
    print_status "Created system_info.sh"
    
    print_status "Management scripts created"
}

# ============================================================================
# DOCUMENTATION
# ============================================================================

create_documentation() {
    print_header "Generating Documentation"
    
    cat > README.md << EOF
# TossIt Distributed Storage Cluster

Installation completed successfully!

## Your Configuration

**Installation Mode:** $INSTALL_MODE
**Local IP:** $LOCAL_IP

EOF

    if [[ "$INSTALL_MODE" == "brain" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        cat >> README.md << EOF
### Brain Server
- Port: $BRAIN_PORT
- Chunk Size: ${CHUNK_SIZE}MB
- Replicas: ${MIN_REPLICAS}-${MAX_REPLICAS}
- Dashboard: http://${LOCAL_IP}:${BRAIN_PORT}

EOF
    fi

    if [[ "$INSTALL_MODE" == "node" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        cat >> README.md << EOF
### Storage Node
- Name: $NODE_NAME
- Port: $NODE_PORT
- Storage Mode: $STORAGE_MODE
EOF

        if [ "$STORAGE_MODE" = "percentage" ]; then
            cat >> README.md << EOF
- Storage Allocation: ${STORAGE_PERCENT}%
EOF
        elif [ "$STORAGE_MODE" = "fixed_gb" ]; then
            cat >> README.md << EOF
- Storage Allocation: ${STORAGE_GB}GB
EOF
        fi

        cat >> README.md << EOF

EOF
    fi

    cat >> README.md << 'EOF'

## Next Steps

### 1. Copy TossIt Source Files

Copy your TossIt Python source files to this directory:

```bash
cp /path/to/tossit/*.py .
```

Required files:
- brain_server.py (if running brain)
- node_agent.py (if running node)
- models.py
- tossit_settings.py
- tossit_debug_logger.py
- tossit_rebalancer.py

### 2. Start Services

EOF

    if [[ "$INSTALL_MODE" == "brain" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        cat >> README.md << EOF
**Start Brain Server:**
\`\`\`bash
./start_brain.sh
\`\`\`

EOF
    fi

    if [[ "$INSTALL_MODE" == "node" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        cat >> README.md << EOF
**Start Storage Node:**
\`\`\`bash
./start_node.sh
\`\`\`

EOF
    fi

    cat >> README.md << 'EOF'

### 3. Verify Installation

```bash
./check_status.sh
./system_info.sh
```

## Management Commands

| Command | Description |
|---------|-------------|
| `./start_brain.sh` | Start brain server |
| `./start_node.sh` | Start storage node |
| `./check_status.sh` | Check service status |
| `./system_info.sh` | Display system info |

## Configuration Files

Your configuration files are in the `config/` directory:
EOF

    if [[ "$INSTALL_MODE" == "brain" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        echo "- \`config/brain_config.json\` - Brain server settings" >> README.md
    fi

    if [[ "$INSTALL_MODE" == "node" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        echo "- \`config/${NODE_NAME}_config.json\` - Node settings" >> README.md
    fi

    cat >> README.md << 'EOF'

## Directory Structure

```
tossit/
├── config/          # Configuration files
├── storage/         # Chunk storage
├── logs/            # Application logs
├── debug_logs/      # Upload debug logs
├── backups/         # Database backups
├── venv/            # Python virtual environment
├── *.py             # TossIt source files (copy here)
└── *.sh             # Management scripts
```

## Troubleshooting

### Services won't start
1. Ensure source files are copied: `ls *.py`
2. Check virtual environment: `source venv/bin/activate`
3. Verify configuration: `cat config/*.json`

### Network connectivity issues
1. Check firewall settings for configured ports
2. Verify IP addresses in config files
3. Test connectivity: `curl http://[brain-ip]:[port]/health`

### Python errors
1. Activate environment: `source venv/bin/activate`
2. Verify packages: `pip list`
3. Reinstall if needed: `pip install -r requirements.txt`

## Support

For issues, check:
- System logs: `./system_info.sh`
- Service status: `./check_status.sh`
- Configuration: `cat config/*.json`
EOF

    print_status "Documentation created: README.md"
}

# ============================================================================
# MAIN INSTALLATION FLOW
# ============================================================================

main() {
    clear
    print_banner
    
    # Get installation directory
    INSTALL_DIR="${1:-$HOME/tossit}"
    
    print_info "TossIt will be installed to: $INSTALL_DIR"
    
    if [ -d "$INSTALL_DIR" ] && [ "$(ls -A $INSTALL_DIR 2>/dev/null)" ]; then
        print_warning "Directory $INSTALL_DIR already exists and is not empty"
        if ! prompt_yes_no "Continue installation?" "y"; then
            print_info "Installation cancelled"
            exit 0
        fi
    fi
    
    # Create and enter installation directory
    mkdir -p "$INSTALL_DIR"
    cd "$INSTALL_DIR"
    
    # Run installation steps
    run_preflight_checks
    install_system_dependencies
    create_directory_structure
    setup_python_environment
    collect_configuration
    generate_config_files
    create_start_scripts
    create_documentation
    
    # Final summary
    print_header "Installation Complete!"
    echo ""
    print_status "TossIt has been successfully installed to: $INSTALL_DIR"
    echo ""
    
    print_info "Next steps:"
    echo "  1. cd $INSTALL_DIR"
    echo "  2. Copy TossIt source files: cp /path/to/tossit/*.py ."
    
    if [[ "$INSTALL_MODE" == "brain" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        echo "  3. Start brain: ./start_brain.sh"
        echo "  4. Access dashboard: http://${LOCAL_IP}:${BRAIN_PORT}"
    fi
    
    if [[ "$INSTALL_MODE" == "node" ]] || [[ "$INSTALL_MODE" == "both" ]]; then
        if [[ "$INSTALL_MODE" == "both" ]]; then
            echo "  5. Start node: ./start_node.sh"
        else
            echo "  3. Start node: ./start_node.sh"
        fi
    fi
    
    echo ""
    print_info "For detailed instructions, see: $INSTALL_DIR/README.md"
    print_status "Happy clustering! 🚀"
}

# ============================================================================
# SCRIPT ENTRY POINT
# ============================================================================

# Trap errors
trap 'print_error "Installation failed at line $LINENO"; exit 1' ERR

# Run main installation
main "$@"

# ============================================================================
# MAIN INSTALLATION FLOW
# ============================================================================

main() {
    clear
    print_banner
    
    # Get installation directory
    INSTALL_DIR="${1:-$HOME/tossit}"
    
    print_info "TossIt will be installed to: $INSTALL_DIR"
    
    if [ -d "$INSTALL_DIR" ] && [ "$(ls -A $INSTALL_DIR)" ]; then
        print_warning "Directory $INSTALL_DIR already exists and is not empty"
        if ! prompt_yes_no "Continue installation?" "y"; then
            print_info "Installation cancelled"
            exit 0
        fi
    fi
    
    #
 }
