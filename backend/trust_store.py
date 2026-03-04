"""
trust_store.py - Manages trusted peer identities for TossIt

Implements TOFU (Trust On First Use) security model.
"""

import json
from pathlib import Path
from datetime import datetime, timezone
from node_identity import NodeIdentity


class TrustStore:
    """
    Stores and manages trusted peer public keys
    
    Trust Model: TOFU (Trust On First Use)
    - First time seeing a node: Trust it automatically
    - Subsequent messages: Verify against stored public key
    - Key mismatch: Reject (possible attack or key rotation)
    """
    
    def __init__(self, trust_store_path: Path):
        """
        Initialize trust store
        
        Args:
            trust_store_path: Path to JSON file storing trusted peers
        """
        self.trust_store_path = Path(trust_store_path)
        self.trusted_peers = {}  # node_id -> {public_key, first_seen, last_seen, name}
        self._load_trust_store()
    
    def _load_trust_store(self):
        """Load trusted peers from disk"""
        if self.trust_store_path.exists():
            try:
                with open(self.trust_store_path, 'r') as f:
                    self.trusted_peers = json.load(f)
                print(f"Loaded {len(self.trusted_peers)} trusted peer(s)")
                
                # Print trusted peers
                for node_id, info in self.trusted_peers.items():
                    print(f"• {info['node_name']} ({node_id[:8]}...)")
                    
            except Exception as e:
                print(f"Failed to load trust store: {e}")
                print(f"Starting with empty trust store")
                self.trusted_peers = {}
        else:
            print(f"No trust store found at {self.trust_store_path}")
            print(f"Starting with empty trust store (will trust on first use)")
    
    def _save_trust_store(self):
        """Save trusted peers to disk"""
        try:
            # Create parent directory if needed
            self.trust_store_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write atomically (write to temp file, then rename)
            temp_path = self.trust_store_path.with_suffix('.tmp')
            with open(temp_path, 'w') as f:
                json.dump(self.trusted_peers, f, indent=2, sort_keys=True)
            
            # Atomic rename
            temp_path.replace(self.trust_store_path)
            
        except Exception as e:
            print(f"Failed to save trust store: {e}")
    
    def is_trusted(self, node_id: str) -> bool:
        """Check if a node ID is in the trust store"""
        return node_id in self.trusted_peers
    
    def trust_peer(self, node_id: str, public_key_b64: str, node_name: str, force: bool = False):
        """
        Trust a peer (TOFU model)
        
        Args:
            node_id: Cryptographic node ID (derived from public key)
            public_key_b64: Base64-encoded public key
            node_name: Human-readable node name
            force: If True, overwrite existing trust (for key rotation)
        """
        now = datetime.now(timezone.utc).isoformat()
        
        if node_id in self.trusted_peers and not force:
            # Already trusted, just update last_seen
            self.trusted_peers[node_id]['last_seen'] = now
            self._save_trust_store()
            return
        
        # First time seeing this node (or forced update)
        if node_id in self.trusted_peers and force:
            old_key = self.trusted_peers[node_id]['public_key']
            print(f"REPLACING trust for {node_name} ({node_id[:8]}...)")
            print(f"Old key: {old_key[:32]}...")
            print(f"New key: {public_key_b64[:32]}...")
        else:
            print(f"Trusting new peer: {node_name} ({node_id[:8]}...)")
            print(f"Public key: {public_key_b64[:32]}...")
            print(f"Using TOFU: Trusting on first contact")
        
        self.trusted_peers[node_id] = {
            'public_key': public_key_b64,
            'node_name': node_name,
            'first_seen': now,
            'last_seen': now
        }
        
        self._save_trust_store()
    
    def untrust_peer(self, node_id: str):
        """
        Remove trust for a peer
        
        Use cases:
        - Node decommissioned
        - Key compromised
        - Security breach
        
        Args:
            node_id: Node ID to untrust
        """
        if node_id in self.trusted_peers:
            node_name = self.trusted_peers[node_id]['node_name']
            del self.trusted_peers[node_id]
            self._save_trust_store()
            print(f"Removed trust for {node_name} ({node_id[:8]}...)")
            print(f"Future messages from this node will be rejected")
        else:
            print(f"Node {node_id[:8]}... not in trust store")
    
    def get_public_key(self, node_id: str) -> str:
        """
        Get stored public key for a node
        
        Returns:
            Base64-encoded public key, or None if not trusted
        """
        if node_id in self.trusted_peers:
            return self.trusted_peers[node_id]['public_key']
        return None
    
    def get_node_name(self, node_id: str) -> str:
        """Get human-readable name for a node ID"""
        if node_id in self.trusted_peers:
            return self.trusted_peers[node_id]['node_name']
        return "unknown"
    
    def verify_message(self, signed_data: dict) -> tuple[bool, dict, str]:
        """
        Verify a signed message from a peer
        
        This is the main entry point for verifying inter-node messages.
        
        Process:
        1. Verify signature is mathematically valid
        2. Extract node ID and public key
        3. Check if node is trusted (TOFU if first time)
        4. Verify public key matches stored key (if already trusted)
        
        Args:
            signed_data: Dictionary with 'message', 'signature', 'public_key', 'node_id'
        
        Returns:
            (verified, message, node_id)
            - verified: True if message is authentic
            - message: Decoded message (if verified)
            - node_id: Sender's node ID (if verified)
        """
        # Step 1: Verify signature is mathematically valid
        valid, message = NodeIdentity.verify_signed_json(signed_data)
        
        if not valid:
            print(f"Signature verification failed: Invalid signature")
            return False, None, None
        
        # Extract identity
        node_id = signed_data['node_id']
        public_key_b64 = signed_data['public_key']
        node_name = message.get('node_name', 'unknown')
        
        # Step 2: Check trust status
        if not self.is_trusted(node_id):
            # TOFU: Trust on first use
            print(f"First time seeing node {node_name} ({node_id[:8]}...)")
            print(f"Public key: {public_key_b64[:32]}...")
            print(f"Trusting based on TOFU policy")
            
            self.trust_peer(node_id, public_key_b64, node_name)
            return True, message, node_id
        
        # Step 3: Verify public key matches stored key
        stored_key = self.get_public_key(node_id)
        
        if stored_key != public_key_b64:
            # PUBLIC KEY MISMATCH - This is serious!
            print(f"PUBLIC KEY MISMATCH for {node_name} ({node_id[:8]}...)!")
            print(f"Stored key:   {stored_key[:32]}...")
            print(f"Received key: {public_key_b64[:32]}...")
            print(f"")
            print(f"This could be:")
            print(f"1. Legitimate key rotation (use --force-trust if intentional)")
            print(f"2. Man-in-the-middle attack")
            print(f"3. Node reinstalled without backing up keys")
            print(f"4. Someone trying to impersonate this node")
            print(f"")
            print(f"REJECTING message for security")
            return False, None, None
        
        # All checks passed!
        # Update last_seen timestamp
        self.trust_peer(node_id, public_key_b64, node_name)
        
        return True, message, node_id
    
    def list_trusted_peers(self) -> list[dict]:
        """
        Get list of all trusted peers
        
        Returns:
            List of dictionaries with node info
        """
        result = []
        for node_id, info in self.trusted_peers.items():
            result.append({
                'node_id': node_id,
                'node_name': info['node_name'],
                'public_key': info['public_key'],
                'first_seen': info['first_seen'],
                'last_seen': info['last_seen']
            })
        return result


# CLI tool for trust management
if __name__ == "__main__":
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description="TossIt Trust Store Manager")
    parser.add_argument('--trust-store', default='~/.tossit/trust_store.json',
                       help='Path to trust store JSON file')
    
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List trusted peers')
    
    # Trust command
    trust_parser = subparsers.add_parser('trust', help='Trust a peer')
    trust_parser.add_argument('node_id', help='Node ID to trust')
    trust_parser.add_argument('public_key', help='Base64-encoded public key')
    trust_parser.add_argument('node_name', help='Human-readable node name')
    trust_parser.add_argument('--force', action='store_true', help='Force trust (replace existing)')
    
    # Untrust command
    untrust_parser = subparsers.add_parser('untrust', help='Untrust a peer')
    untrust_parser.add_argument('node_id', help='Node ID to untrust')
    
    args = parser.parse_args()
    
    # Expand ~ in path
    trust_store_path = Path(args.trust_store).expanduser()
    trust_store = TrustStore(trust_store_path)
    
    if args.command == 'list':
        peers = trust_store.list_trusted_peers()
        if not peers:
            print("No trusted peers")
        else:
            print(f"\nTrusted peers ({len(peers)}):\n")
            for peer in peers:
                print(f"{peer['node_name']}")
                print(f"Node ID:    {peer['node_id']}")
                print(f"Public key: {peer['public_key'][:32]}...")
                print(f"First seen: {peer['first_seen']}")
                print(f"Last seen:  {peer['last_seen']}")
                print()
    
    elif args.command == 'trust':
        trust_store.trust_peer(
            node_id=args.node_id,
            public_key_b64=args.public_key,
            node_name=args.node_name,
            force=args.force
        )
    
    elif args.command == 'untrust':
        trust_store.untrust_peer(args.node_id)
    
    else:
        parser.print_help()
        sys.exit(1)
