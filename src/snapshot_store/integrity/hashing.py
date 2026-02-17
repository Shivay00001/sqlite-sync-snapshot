"""
Content-addressed hashing using BLAKE3 or SHA-256.

Provides deterministic hash computation for all object types.
"""

import hashlib
from typing import Any

try:
    import blake3
    HAS_BLAKE3 = True
except ImportError:
    HAS_BLAKE3 = False

from .canonical import canonical_json


def compute_hash(data: bytes) -> str:
    """
    Compute hash of raw bytes.
    
    Uses BLAKE3 if available, otherwise SHA-256.
    Returns hex-encoded hash string.
    """
    if HAS_BLAKE3:
        return blake3.blake3(data).hexdigest()
    else:
        return hashlib.sha256(data).hexdigest()


def compute_object_hash(obj: Any) -> str:
    """
    Compute hash of a structured object using canonical JSON encoding.
    
    This ensures deterministic hashing:
    - Same object structure always produces same hash
    - Independent of Python dict ordering
    - Independent of timestamp or random values in metadata
    
    Returns hex-encoded hash string.
    """
    canonical_bytes = canonical_json(obj)
    return compute_hash(canonical_bytes)


def verify_hash(data: bytes, expected_hash: str) -> bool:
    """
    Verify that data matches expected hash.
    
    Returns True if match, False otherwise.
    """
    actual_hash = compute_hash(data)
    return actual_hash == expected_hash


def compute_content_hash(content: bytes, object_type: str, metadata: dict = None) -> str:
    """
    Compute hash for an object with type, content, and metadata.
    
    The hash is computed over a canonical representation that includes:
    - object type
    - content (base64 or nested structure)
    - metadata (if provided)
    
    This is used for creating content-addressed objects.
    """
    import base64
    
    obj = {
        "type": object_type,
        "content": base64.b64encode(content).decode('ascii') if isinstance(content, bytes) else content,
    }
    
    if metadata:
        obj["metadata"] = metadata
    
    return compute_object_hash(obj)


def get_hash_prefix(hash_str: str, prefix_length: int = 2) -> str:
    """
    Get prefix of hash for directory sharding.
    
    Default is 2 characters, creating 256 subdirectories.
    """
    if len(hash_str) < prefix_length:
        raise ValueError(f"Hash too short for prefix length {prefix_length}")
    return hash_str[:prefix_length]
