"""
Blob object model.

Blobs store raw binary data content-addressed by hash.
"""

import base64
from typing import Optional
from ..integrity.hashing import compute_object_hash


class Blob:
    """
    Immutable blob object containing raw data.
    
    Blobs are leaf objects - they contain no references.
    """
    
    def __init__(self, data: bytes, metadata: Optional[dict] = None):
        """
        Create a blob from raw bytes.
        
        Args:
            data: raw binary data
            metadata: optional metadata dict
        """
        self.data = data
        self.metadata = metadata or {}
    
    def to_dict(self) -> dict:
        """
        Convert blob to storable dictionary representation.
        
        Returns canonical dict that can be hashed and stored.
        """
        obj = {
            'type': 'blob',
            'content': base64.b64encode(self.data).decode('ascii'),
        }
        
        if self.metadata:
            obj['metadata'] = self.metadata
        
        return obj
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Blob':
        """
        Reconstruct blob from stored dictionary.
        
        Raises ValueError if data is invalid.
        """
        if data.get('type') != 'blob':
            raise ValueError(f"Invalid blob type: {data.get('type')}")
        
        if 'content' not in data:
            raise ValueError("Blob missing content field")
        
        # Decode base64 content
        try:
            content_bytes = base64.b64decode(data['content'])
        except Exception as e:
            raise ValueError(f"Failed to decode blob content: {e}")
        
        metadata = data.get('metadata', {})
        
        return cls(content_bytes, metadata)
    
    def compute_hash(self) -> str:
        """Compute content hash of this blob."""
        return compute_object_hash(self.to_dict())
    
    def size(self) -> int:
        """Get size of blob data in bytes."""
        return len(self.data)
    
    def __repr__(self) -> str:
        size = len(self.data)
        hash_preview = self.compute_hash()[:8]
        return f"Blob(size={size}, hash={hash_preview}...)"
