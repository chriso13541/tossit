"""
node_identity.py - Cryptographic identity for TossIt nodes

Provides Ed25519 key pair generation, message signing, and verification.
"""

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
from pathlib import Path
import base64
import json
import hashlib


class NodeIdentity:
    """
    Cryptographic identity for a TossIt node
    
    Features:
    - Ed25519 public/private key pair
    - Persistent key storage
    - Message signing and verification
    - Cryptographic node ID derivation
    """
    
    def __init__(self, node_name: str, keys_path: Path):
        """
        Initialize node identity
        
        Args:
            node_name: Human-readable node name (e.g., "temple")
            keys_path: Directory to store keys (e.g., ~/.tossit/keys/temple)
        """
        self.node_name = node_name
        self.keys_path = Path(keys_path)
        self.private_key = None
        self.public_key = None
        
        # Load or generate keys
        self._load_or_generate_keys()
    
    def _load_or_generate_keys(self):
        """Load existing keys or generate new ones"""
        private_key_path = self.keys_path / "node_private.pem"
        public_key_path = self.keys_path / "node_public.pem"
        
        if private_key_path.exists() and public_key_path.exists():
            # Load existing keys
            try:
                with open(private_key_path, 'rb') as f:
                    self.private_key = serialization.load_pem_private_key(
                        f.read(),
                        password=None
                    )
                
                with open(public_key_path, 'rb') as f:
                    self.public_key = serialization.load_pem_public_key(f.read())
                
                print(f"🔑 Loaded existing keys for {self.node_name}")
                print(f"   Key location: {self.keys_path}")
                
            except Exception as e:
                print(f"❌ Failed to load keys: {e}")
                print(f"   Generating new keys...")
                self._generate_new_keys(private_key_path, public_key_path)
        else:
            # Generate new keys
            self._generate_new_keys(private_key_path, public_key_path)
    
    def _generate_new_keys(self, private_key_path: Path, public_key_path: Path):
        """Generate new Ed25519 key pair and save to disk"""
        # Generate new keys
        self.private_key = ed25519.Ed25519PrivateKey.generate()
        self.public_key = self.private_key.public_key()
        
        # Create directory
        self.keys_path.mkdir(parents=True, exist_ok=True)
        
        # Save private key
        with open(private_key_path, 'wb') as f:
            f.write(self.private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            ))
        
        # Save public key
        with open(public_key_path, 'wb') as f:
            f.write(self.public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            ))
        
        # Secure permissions
        private_key_path.chmod(0o600)  # Owner read/write only
        public_key_path.chmod(0o644)   # Owner read/write, others read
        
        print(f"🔑 Generated new keys for {self.node_name}")
        print(f"   Private key: {private_key_path} (600)")
        print(f"   Public key:  {public_key_path} (644)")
        print(f"   ⚠️  BACKUP THESE KEYS! Loss means loss of identity.")
    
    def get_public_key_bytes(self) -> bytes:
        """Get public key as raw bytes (32 bytes for Ed25519)"""
        return self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw
        )
    
    def get_public_key_base64(self) -> str:
        """Get public key as base64 string (for transmission over network)"""
        return base64.b64encode(self.get_public_key_bytes()).decode('utf-8')
    
    def get_node_id(self) -> str:
        """
        Get cryptographic node ID (derived from public key)
        
        Returns first 16 characters of SHA256(public_key).
        This ensures node ID is tied to cryptographic identity.
        """
        pub_key_bytes = self.get_public_key_bytes()
        node_id = hashlib.sha256(pub_key_bytes).hexdigest()[:16]
        return node_id
    
    def sign_message(self, message: bytes) -> bytes:
        """
        Sign a message with private key
        
        Args:
            message: Raw bytes to sign
        
        Returns:
            64-byte Ed25519 signature
        """
        signature = self.private_key.sign(message)
        return signature
    
    def sign_json(self, data: dict) -> dict:
        """
        Sign a JSON message and return signed envelope
        
        Args:
            data: Dictionary to sign (will be serialized to JSON)
        
        Returns:
            Dictionary with 'message', 'signature', 'public_key', 'node_id'
        """
        # Serialize message to canonical JSON (sorted keys for consistency)
        message_bytes = json.dumps(data, sort_keys=True).encode('utf-8')
        
        # Sign
        signature = self.sign_message(message_bytes)
        
        # Return signed envelope
        return {
            'message': data,
            'signature': base64.b64encode(signature).decode('utf-8'),
            'public_key': self.get_public_key_base64(),
            'node_id': self.get_node_id()
        }
    
    @staticmethod
    def verify_signature(public_key_base64: str, message: bytes, signature: bytes) -> bool:
        """
        Verify a signature against a public key
        
        Args:
            public_key_base64: Base64-encoded public key
            message: Original message bytes
            signature: Signature bytes
        
        Returns:
            True if signature is valid, False otherwise
        """
        try:
            # Decode public key
            public_key_bytes = base64.b64decode(public_key_base64)
            public_key = ed25519.Ed25519PublicKey.from_public_bytes(public_key_bytes)
            
            # Verify signature (raises exception if invalid)
            public_key.verify(signature, message)
            return True
            
        except Exception as e:
            # Signature verification failed
            return False
    
    @staticmethod
    def verify_signed_json(signed_data: dict) -> tuple[bool, dict]:
        """
        Verify a signed JSON message
        
        Args:
            signed_data: Dictionary with 'message', 'signature', 'public_key', 'node_id'
        
        Returns:
            (verified, message) - verified is True if signature valid
        """
        try:
            # Extract components
            message = signed_data['message']
            signature_b64 = signed_data['signature']
            public_key_b64 = signed_data['public_key']
            claimed_node_id = signed_data['node_id']
            
            # Verify node ID matches public key
            public_key_bytes = base64.b64decode(public_key_b64)
            actual_node_id = hashlib.sha256(public_key_bytes).hexdigest()[:16]
            
            if actual_node_id != claimed_node_id:
                print(f"❌ Node ID mismatch: claimed={claimed_node_id}, actual={actual_node_id}")
                return False, None
            
            # Recreate message bytes (canonical JSON)
            message_bytes = json.dumps(message, sort_keys=True).encode('utf-8')
            signature = base64.b64decode(signature_b64)
            
            # Verify signature
            if NodeIdentity.verify_signature(public_key_b64, message_bytes, signature):
                return True, message
            else:
                print(f"❌ Signature verification failed")
                return False, None
        
        except KeyError as e:
            print(f"❌ Missing required field in signed message: {e}")
            return False, None
        except Exception as e:
            print(f"❌ Signature verification error: {e}")
            return False, None


# Example usage
if __name__ == "__main__":
    import tempfile
    
    print("=== NodeIdentity Demo ===\n")
    
    # Create identity
    with tempfile.TemporaryDirectory() as tmpdir:
        keys_path = Path(tmpdir) / "test-keys"
        identity = NodeIdentity("test-node", keys_path)
        
        print(f"\nNode ID: {identity.get_node_id()}")
        print(f"Public key: {identity.get_public_key_base64()}\n")
        
        # Sign a message
        message = {
            "type": "heartbeat",
            "from": "test-node",
            "timestamp": 123456
        }
        
        print(f"Original message: {message}")
        
        signed = identity.sign_json(message)
        print(f"\nSigned envelope:")
        print(f"  Node ID: {signed['node_id']}")
        print(f"  Signature: {signed['signature'][:32]}...")
        
        # Verify
        valid, decoded = NodeIdentity.verify_signed_json(signed)
        print(f"\nVerification result: {'✅ VALID' if valid else '❌ INVALID'}")
        print(f"Decoded message: {decoded}")
        
        # Try to tamper
        print("\n=== Tampering attempt ===")
        tampered = signed.copy()
        tampered['message']['timestamp'] = 999999  # Change message
        
        valid, decoded = NodeIdentity.verify_signed_json(tampered)
        print(f"Tampered verification: {'✅ VALID' if valid else '❌ INVALID (as expected)'}")
