"""
TossIt Configuration Manager - Enhanced Version
Handles loading, validating, and generating configuration files for Brain and Node agents
Supports auto-detection and interactive configuration generation
"""

import json
import os
import socket
from pathlib import Path
from typing import Optional, Dict, Any
import sys


class TossItConfig:
    """Configuration manager for TossIt nodes and brain"""
    
    # Default configuration with runtime path resolution
    @staticmethod
    def get_default_config():
        """Generate default configuration with proper paths"""
        home_dir = os.path.expanduser('~')
        install_dir = os.path.join(home_dir, 'tossit')
        
        # Try to detect local IP
        local_ip = TossItConfig._detect_local_ip()
        
        return {
            "node": {
                "name": socket.gethostname(),
                "storage_path": os.path.join(install_dir, "storage"),
                "port": 8080,
                "brain_url": f"http://{local_ip}:8000",
                "storage_mode": "percentage",
                "storage_limit_percent": 50,
                "storage_limit_gb": 100,
                "auto_register": True,
                "heartbeat_interval": 30,
                "job_check_interval": 5
            },
            "brain": {
                "ip_address": local_ip,
                "port": 8000,
                "database_path": "./tossit_brain.db",
                "chunk_size_mb": 64,
                "min_replicas": 2,
                "max_replicas": 3,
                "enable_web_ui": True
            },
            "cluster": {
                "heartbeat_interval_seconds": 30,
                "job_check_interval_seconds": 5,
                "replication_priority": 7,
                "verification_interval_hours": 24,
                "auto_rebalance_enabled": True,
                "auto_rebalance_threshold": 80,
                "max_migrations_per_rebalance": 50
            }
        }
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize configuration, optionally loading from file"""
        self.config = self.get_default_config()
        self.config_path = config_path
        
        if config_path:
            self.load_from_file(config_path)
    
    @staticmethod
    def _detect_local_ip() -> str:
        """Detect the local IP address"""
        try:
            # Create a socket to determine local IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            s.close()
            return local_ip
        except Exception:
            # Fallback to localhost
            return "127.0.0.1"
    
    def load_from_file(self, config_path: str) -> bool:
        """Load configuration from JSON file"""
        path = Path(config_path)
        
        if not path.exists():
            print(f"⚠️  Config file not found: {config_path}")
            print("   Using default configuration")
            return False
        
        try:
            with open(path, 'r') as f:
                user_config = json.load(f)
            
            # Deep merge user config with defaults
            self._merge_config(self.config, user_config)
            self.config_path = config_path
            print(f"✓ Loaded configuration from {config_path}")
            return True
            
        except json.JSONDecodeError as e:
            print(f"✗ Invalid JSON in config file: {e}")
            print("  Using default configuration")
            return False
        except Exception as e:
            print(f"✗ Error loading config: {e}")
            print("  Using default configuration")
            return False
    
    def _merge_config(self, base: dict, updates: dict):
        """Recursively merge updates into base config"""
        for key, value in updates.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_config(base[key], value)
            else:
                base[key] = value
    
    def save_to_file(self, config_path: Optional[str] = None) -> bool:
        """Save current configuration to JSON file"""
        path = Path(config_path or self.config_path or "tossit_config.json")
        
        try:
            # Create parent directory if needed
            path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(path, 'w') as f:
                json.dump(self.config, f, indent=2)
            
            print(f"✓ Configuration saved to {path}")
            return True
        except Exception as e:
            print(f"✗ Error saving config: {e}")
            return False
    
    def get_node_config(self) -> Dict[str, Any]:
        """Get node-specific configuration"""
        return self.config.get("node", {})
    
    def get_brain_config(self) -> Dict[str, Any]:
        """Get brain-specific configuration"""
        return self.config.get("brain", {})
    
    def get_cluster_config(self) -> Dict[str, Any]:
        """Get cluster-wide configuration"""
        return self.config.get("cluster", {})
    
    def validate(self) -> tuple[bool, list[str]]:
        """Validate configuration and return (is_valid, errors)"""
        errors = []
        
        # Validate node config
        node = self.get_node_config()
        if node:
            if not node.get("name"):
                errors.append("Node name is required")
            
            if not node.get("storage_path"):
                errors.append("Storage path is required")
            
            port = node.get("port", 0)
            if not (1024 <= port <= 65535):
                errors.append(f"Invalid node port: {port} (must be 1024-65535)")
            
            if not node.get("brain_url"):
                errors.append("Brain URL is required for nodes")
            
            storage_mode = node.get("storage_mode", "percentage")
            if storage_mode not in ["percentage", "fixed_gb", "full"]:
                errors.append(f"Invalid storage mode: {storage_mode}")
        
        # Validate brain config
        brain = self.get_brain_config()
        if brain:
            port = brain.get("port", 0)
            if not (1024 <= port <= 65535):
                errors.append(f"Invalid brain port: {port} (must be 1024-65535)")
            
            chunk_size = brain.get("chunk_size_mb", 0)
            if chunk_size < 1 or chunk_size > 1024:
                errors.append(f"Invalid chunk size: {chunk_size}MB (must be 1-1024)")
            
            min_replicas = brain.get("min_replicas", 0)
            max_replicas = brain.get("max_replicas", 0)
            if min_replicas < 1 or min_replicas > max_replicas:
                errors.append(f"Invalid replica range: {min_replicas}-{max_replicas}")
        
        return (len(errors) == 0, errors)
    
    def calculate_storage_limit(self, total_available_gb: float) -> float:
        """Calculate actual storage limit based on configuration"""
        node_config = self.get_node_config()
        mode = node_config.get("storage_mode", "percentage")
        
        if mode == "full":
            # Use 95% as safety margin
            return total_available_gb * 0.95
        elif mode == "percentage":
            percent = node_config.get("storage_limit_percent", 50)
            limit = total_available_gb * (percent / 100.0)
            # Cap at 95% for safety
            return min(limit, total_available_gb * 0.95)
        elif mode == "fixed_gb":
            fixed = node_config.get("storage_limit_gb", 100)
            # Don't exceed available space (with 95% cap)
            return min(fixed, total_available_gb * 0.95)
        else:
            print(f"⚠️  Unknown storage mode: {mode}, using 50% default")
            return total_available_gb * 0.5
    
    def print_summary(self):
        """Print human-readable configuration summary"""
        print("\n" + "="*70)
        print("TossIt Configuration Summary")
        print("="*70)
        
        node = self.get_node_config()
        if node:
            print("\n📦 Node Configuration:")
            print(f"  Name:         {node.get('name')}")
            print(f"  Storage Path: {node.get('storage_path')}")
            print(f"  Port:         {node.get('port')}")
            print(f"  Brain URL:    {node.get('brain_url')}")
            print(f"  Storage Mode: {node.get('storage_mode')}")
            
            mode = node.get('storage_mode')
            if mode == "percentage":
                print(f"  Allocation:   {node.get('storage_limit_percent')}% of available")
            elif mode == "fixed_gb":
                print(f"  Allocation:   {node.get('storage_limit_gb')}GB")
            elif mode == "full":
                print(f"  Allocation:   All available space (95% safety cap)")
        
        brain = self.get_brain_config()
        if brain:
            print("\n🧠 Brain Configuration:")
            print(f"  IP Address:   {brain.get('ip_address')}")
            print(f"  Port:         {brain.get('port')}")
            print(f"  Dashboard:    http://{brain.get('ip_address')}:{brain.get('port')}")
            print(f"  Chunk Size:   {brain.get('chunk_size_mb')}MB")
            print(f"  Replicas:     {brain.get('min_replicas')}-{brain.get('max_replicas')}")
            print(f"  Database:     {brain.get('database_path')}")
        
        cluster = self.get_cluster_config()
        if cluster:
            print("\n🌐 Cluster Configuration:")
            print(f"  Heartbeat:    Every {cluster.get('heartbeat_interval_seconds')}s")
            print(f"  Job Check:    Every {cluster.get('job_check_interval_seconds')}s")
            print(f"  Verification: Every {cluster.get('verification_interval_hours')}h")
            if cluster.get('auto_rebalance_enabled'):
                print(f"  Auto-rebalance: Enabled (threshold: {cluster.get('auto_rebalance_threshold')}%)")
        
        print("="*70 + "\n")
    
    @staticmethod
    def create_interactive(save_path: Optional[str] = None) -> 'TossItConfig':
        """Create configuration interactively"""
        print("\n" + "="*70)
        print("TossIt Interactive Configuration")
        print("="*70 + "\n")
        
        config = TossItConfig()
        
        # Mode selection
        print("What would you like to configure?")
        print("  1) Brain Server (central coordinator)")
        print("  2) Storage Node (distributed storage)")
        print("  3) Both (brain + storage on same machine)")
        
        mode = input("\nSelect mode [1-3] (default: 3): ").strip() or "3"
        
        # Network detection
        local_ip = TossItConfig._detect_local_ip()
        print(f"\n🌐 Auto-detected IP address: {local_ip}")
        use_detected = input("Use this IP? [Y/n]: ").strip().lower()
        
        if use_detected not in ['', 'y', 'yes']:
            local_ip = input("Enter IP address: ").strip() or local_ip
        
        # Brain configuration
        if mode in ['1', '3']:
            print("\n🧠 Brain Server Configuration:")
            
            brain_port = input(f"  Port (default: 8000): ").strip()
            chunk_size = input(f"  Chunk size in MB (default: 64): ").strip()
            min_replicas = input(f"  Minimum replicas (default: 2): ").strip()
            max_replicas = input(f"  Maximum replicas (default: 3): ").strip()
            
            config.config["brain"]["ip_address"] = local_ip
            config.config["brain"]["port"] = int(brain_port) if brain_port else 8000
            config.config["brain"]["chunk_size_mb"] = int(chunk_size) if chunk_size else 64
            config.config["brain"]["min_replicas"] = int(min_replicas) if min_replicas else 2
            config.config["brain"]["max_replicas"] = int(max_replicas) if max_replicas else 3
        
        # Node configuration
        if mode in ['2', '3']:
            print("\n📦 Storage Node Configuration:")
            
            node_name = input(f"  Node name (default: {socket.gethostname()}): ").strip()
            node_port = input(f"  Port (default: {'8081' if mode == '3' else '8080'}): ").strip()
            
            if mode == '3':
                brain_url = f"http://{local_ip}:{config.config['brain']['port']}"
            else:
                brain_url = input(f"  Brain URL (e.g., http://192.168.1.100:8000): ").strip()
            
            print("\n  Storage Modes:")
            print("    1) Percentage - Use a percentage of available space")
            print("    2) Fixed GB - Use a fixed amount of space")
            print("    3) Full - Use all available space (not recommended)")
            
            storage_mode = input("\n  Select storage mode [1-3] (default: 1): ").strip() or "1"
            
            config.config["node"]["name"] = node_name or socket.gethostname()
            config.config["node"]["port"] = int(node_port) if node_port else (8081 if mode == '3' else 8080)
            config.config["node"]["brain_url"] = brain_url
            
            if storage_mode == "1":
                config.config["node"]["storage_mode"] = "percentage"
                percent = input("    Percentage to use (default: 50): ").strip()
                config.config["node"]["storage_limit_percent"] = int(percent) if percent else 50
            elif storage_mode == "2":
                config.config["node"]["storage_mode"] = "fixed_gb"
                gb = input("    Fixed GB amount (default: 100): ").strip()
                config.config["node"]["storage_limit_gb"] = int(gb) if gb else 100
            else:
                config.config["node"]["storage_mode"] = "full"
                print("    ⚠️  Warning: This will use all available space!")
        
        # Validate configuration
        is_valid, errors = config.validate()
        
        if not is_valid:
            print("\n⚠️  Configuration validation warnings:")
            for error in errors:
                print(f"  - {error}")
            print()
        
        # Summary
        config.print_summary()
        
        # Save option
        if save_path or input("Save this configuration? [Y/n]: ").strip().lower() in ['', 'y', 'yes']:
            save_path = save_path or input("Save to (default: tossit_config.json): ").strip() or "tossit_config.json"
            config.save_to_file(save_path)
        
        return config
    
    @staticmethod
    def create_example_config(output_path: str = "tossit_config_example.json"):
        """Create a well-documented example configuration file"""
        
        local_ip = TossItConfig._detect_local_ip()
        hostname = socket.gethostname()
        
        example = {
            "_comment": "TossIt Configuration File - Edit values below",
            "_documentation": "See https://github.com/yourrepo/tossit for full documentation",
            
            "node": {
                "_comment": "Storage Node Configuration (delete this section if brain-only)",
                "name": f"{hostname}-storage",
                "storage_path": "./storage",
                "port": 8080,
                "brain_url": f"http://{local_ip}:8000",
                
                "storage_mode": "percentage",
                "_storage_mode_options": ["percentage", "fixed_gb", "full"],
                "_storage_mode_info": "percentage: Use % of disk | fixed_gb: Use fixed amount | full: Use all space",
                
                "storage_limit_percent": 50,
                "_storage_limit_percent_info": "Used when storage_mode=percentage (1-100)",
                
                "storage_limit_gb": 100,
                "_storage_limit_gb_info": "Used when storage_mode=fixed_gb",
                
                "auto_register": True,
                "_auto_register_info": "Automatically register with brain on startup",
                
                "heartbeat_interval": 30,
                "_heartbeat_interval_info": "Seconds between heartbeats to brain",
                
                "job_check_interval": 5,
                "_job_check_interval_info": "Seconds between checking for new jobs"
            },
            
            "brain": {
                "_comment": "Brain Server Configuration (delete this section if node-only)",
                "ip_address": local_ip,
                "_ip_address_info": "IP address brain will bind to (0.0.0.0 for all interfaces)",
                
                "port": 8000,
                "database_path": "./tossit_brain.db",
                
                "chunk_size_mb": 64,
                "_chunk_size_mb_info": "Size of each chunk in megabytes (1-1024)",
                
                "min_replicas": 2,
                "_min_replicas_info": "Minimum number of copies for each chunk",
                
                "max_replicas": 3,
                "_max_replicas_info": "Maximum number of copies for each chunk",
                
                "enable_web_ui": True,
                "_enable_web_ui_info": "Enable the web dashboard interface"
            },
            
            "cluster": {
                "_comment": "Cluster-wide Configuration",
                
                "heartbeat_interval_seconds": 30,
                "job_check_interval_seconds": 5,
                "replication_priority": 7,
                "_replication_priority_info": "Priority for replication jobs (1-10, higher=more urgent)",
                
                "verification_interval_hours": 24,
                "_verification_interval_hours_info": "How often to verify chunk integrity",
                
                "auto_rebalance_enabled": True,
                "_auto_rebalance_enabled_info": "Automatically rebalance when cluster becomes unbalanced",
                
                "auto_rebalance_threshold": 80,
                "_auto_rebalance_threshold_info": "Balance score below this triggers rebalancing (0-100)",
                
                "max_migrations_per_rebalance": 50,
                "_max_migrations_per_rebalance_info": "Maximum chunks to migrate in one rebalance operation"
            }
        }
        
        try:
            with open(output_path, 'w') as f:
                json.dump(example, f, indent=2)
            
            print(f"✓ Example configuration created: {output_path}")
            print("\n📝 Edit this file and remove lines starting with '_' before use")
            print(f"   Then use: python3 node_agent.py --config {output_path}")
            return True
        except Exception as e:
            print(f"✗ Error creating example config: {e}")
            return False


def print_storage_info(config: TossItConfig, total_available_gb: float):
    """Print detailed storage configuration information"""
    node_config = config.get_node_config()
    mode = node_config.get("storage_mode", "percentage")
    limit = config.calculate_storage_limit(total_available_gb)
    
    print("\n" + "="*70)
    print("Storage Configuration Analysis")
    print("="*70)
    print(f"\n💾 Mode: {mode}")
    print(f"📊 Total Available: {total_available_gb:.1f} GB")
    print(f"✅ TossIt Will Use: {limit:.1f} GB ({(limit/total_available_gb)*100:.1f}%)")
    
    if mode == "percentage":
        configured = node_config.get('storage_limit_percent', 50)
        print(f"⚙️  Configured Percentage: {configured}%")
    elif mode == "fixed_gb":
        configured = node_config.get('storage_limit_gb', 100)
        print(f"⚙️  Configured Limit: {configured} GB")
        if configured > total_available_gb:
            print(f"⚠️  Note: Configured limit exceeds available space, capped at {limit:.1f} GB")
    elif mode == "full":
        print(f"⚠️  Using ALL available space (95% safety cap applied)")
    
    print(f"\n🔒 Safety Buffer: {(total_available_gb - limit):.1f} GB ({((total_available_gb - limit)/total_available_gb)*100:.1f}%)")
    print("="*70 + "\n")


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Command-line interface for configuration management"""
    
    if len(sys.argv) < 2:
        print("TossIt Configuration Manager")
        print("\nUsage:")
        print("  python3 tossit_config.py create-example [output.json]")
        print("  python3 tossit_config.py create-interactive [output.json]")
        print("  python3 tossit_config.py validate <config.json>")
        print("  python3 tossit_config.py show <config.json>")
        print("\nExamples:")
        print("  python3 tossit_config.py create-example my_config.json")
        print("  python3 tossit_config.py create-interactive node1_config.json")
        print("  python3 tossit_config.py validate tossit_config.json")
        print("  python3 tossit_config.py show tossit_config.json")
        sys.exit(0)
    
    command = sys.argv[1]
    
    if command == "create-example":
        output = sys.argv[2] if len(sys.argv) > 2 else "tossit_config_example.json"
        TossItConfig.create_example_config(output)
    
    elif command == "create-interactive":
        output = sys.argv[2] if len(sys.argv) > 2 else None
        TossItConfig.create_interactive(output)
    
    elif command == "validate":
        if len(sys.argv) < 3:
            print("❌ Error: Config file path required")
            print("Usage: python3 tossit_config.py validate <config.json>")
            sys.exit(1)
        
        config = TossItConfig(sys.argv[2])
        is_valid, errors = config.validate()
        
        if is_valid:
            print("✅ Configuration is valid!")
            config.print_summary()
        else:
            print("❌ Configuration has errors:")
            for error in errors:
                print(f"  - {error}")
            sys.exit(1)
    
    elif command == "show":
        if len(sys.argv) < 3:
            print("❌ Error: Config file path required")
            print("Usage: python3 tossit_config.py show <config.json>")
            sys.exit(1)
        
        config = TossItConfig(sys.argv[2])
        config.print_summary()
    
    else:
        print(f"❌ Unknown command: {command}")
        print("Run without arguments for usage information")
        sys.exit(1)


if __name__ == "__main__":
    main()