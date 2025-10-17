# 🗄️ TossIt - Distributed Storage Cluster

A distributed file storage system that automatically chunks and replicates files across multiple nodes for redundancy and high availability.

## ✨ Features

- **🔀 Distributed Storage**: Files automatically split into chunks and distributed across nodes
- **♻️ Automatic Replication**: Configurable redundancy with 2-3 replicas per chunk
- **⚖️ Smart Load Balancing**: Intelligent chunk placement based on node capacity and performance
- **🔧 Self-Healing**: Automatic rebalancing when nodes join or leave the cluster
- **📊 Web Dashboard**: Real-time monitoring and file management interface
- **🐳 Docker Support**: Easy deployment with Docker Compose
- **🔌 REST API**: Full API for programmatic access

## 🚀 Quick Start

### Option 1: Docker (Recommended)
```bash
# Clone the repository
git clone https://github.com/chriso13541/tossit.git
cd tossit/docker

# Start the cluster (1 brain + 3 storage nodes)
docker-compose up -d

# Access the dashboard
open http://localhost:8000
```

### Option 2: Manual Installation
```bash
# Clone the repository
git clone https://github.com/chriso13541/tossit.git
cd tossit

# Run the automated installer
./scripts/tossit_installer.sh

# Follow the prompts to configure your setup
```

## 📖 Documentation

- **[Docker Deployment Guide](docs/DOCKER.md)** - Complete Docker setup
- **[Installation Guide](docs/INSTALLATION.md)** - Manual installation steps
- **[Configuration](docs/CONFIGURATION.md)** - Configuration options
- **[API Reference](docs/API.md)** - REST API documentation

## 🏗️ Architecture
```
┌─────────────────┐
│  Brain Server   │  ← Central coordinator
│  (Port 8000)    │     • Manages metadata
└────────┬────────┘     • Tracks chunks
         │              • Orchestrates jobs
         │              • Web dashboard
    ┌────┴────┬─────────┬─────────┐
    │         │         │         │
┌───▼───┐ ┌───▼───┐ ┌───▼───┐ ┌───▼───┐
│ Node1 │ │ Node2 │ │ Node3 │ │ Node4 │  ← Storage nodes
│ 8081  │ │ 8082  │ │ 8083  │ │ 8084  │     • Store chunks
└───────┘ └───────┘ └───────┘ └───────┘     • Auto-register
                                              • Self-report status
```

## 🛠️ Technology Stack

- **Backend**: Python 3.10+, FastAPI, SQLAlchemy
- **Storage**: SQLite (metadata), Local filesystem (chunks)
- **Networking**: aiohttp for async communication
- **Deployment**: Docker, Docker Compose

## 📊 System Requirements

### Brain Server
- 2GB RAM minimum
- 10GB disk space
- Python 3.10+

### Storage Nodes
- 1GB RAM minimum
- Configurable storage (default: 50% of available)
- Python 3.10+

### Network
- All nodes must communicate on ports 8000-8083

## 🎯 Use Cases

- **Home Lab Storage**: Personal distributed storage across multiple machines
- **Small Business**: Redundant file storage and backup
- **Learning**: Understand distributed systems architecture
- **Development**: Test distributed storage applications

## 🎮 Usage Examples

### Upload a File
```bash
# Via web dashboard
open http://localhost:8000

# Via API
curl -F "file=@myfile.pdf" http://localhost:8000/api/files/upload
```

### Download a File
```bash
# Get file list
curl http://localhost:8000/api/files

# Download file by ID
curl -O http://localhost:8000/api/files/1/download
```

### Check Cluster Status
```bash
# View cluster nodes
curl http://localhost:8000/api/nodes | python3 -m json.tool

# Health check
curl http://localhost:8000/health
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👤 Author

**Chris O** ([@chriso13541](https://github.com/chriso13541))

## 🙏 Acknowledgments

- Inspired by distributed storage systems like Ceph and MinIO
- Built as a learning project to understand distributed systems

## 🐛 Issues & Support

Found a bug or have a question?
- Open an [Issue](https://github.com/chriso13541/tossit/issues)
- Check the [Documentation](docs/)

---

**⭐ If you find this project useful, please give it a star!**
