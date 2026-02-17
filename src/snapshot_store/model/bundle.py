"""
Bundle object model.

Bundles store sqlite-sync-core sync bundles.
"""

from typing import Optional, Any
from ..integrity.hashing import compute_object_hash


class Bundle:
    """
    Immutable bundle object containing a sync bundle from sqlite-sync-core.
    
    Bundles are leaf objects - they contain no references to other objects.
    They store the sync bundle data directly.
    """
    
    def __init__(self, bundle_data: dict, metadata: Optional[dict] = None):
        """
        Create a bundle from sync bundle data.
        
        Args:
            bundle_data: the sync bundle from sqlite-sync-core
            metadata: optional metadata (source, timestamp, etc)
        """
        self.bundle_data = bundle_data
        self.metadata = metadata or {}
    
    def to_dict(self) -> dict:
        """
        Convert bundle to storable dictionary representation.
        
        Returns canonical dict that can be hashed and stored.
        """
        obj = {
            'type': 'bundle',
            'content': self.bundle_data,
        }
        
        if self.metadata:
            obj['metadata'] = self.metadata
        
        return obj
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Bundle':
        """
        Reconstruct bundle from stored dictionary.
        
        Raises ValueError if data is invalid.
        """
        if data.get('type') != 'bundle':
            raise ValueError(f"Invalid bundle type: {data.get('type')}")
        
        if 'content' not in data:
            raise ValueError("Bundle missing content field")
        
        bundle_data = data['content']
        metadata = data.get('metadata', {})
        
        return cls(bundle_data, metadata)
    
    def compute_hash(self) -> str:
        """Compute content hash of this bundle."""
        return compute_object_hash(self.to_dict())
    
    def get_operations(self) -> list:
        """
        Get operations from bundle if present.
        
        Returns empty list if no operations field.
        """
        return self.bundle_data.get('operations', [])
    
    def get_sequence_number(self) -> Optional[int]:
        """Get sequence number from bundle if present."""
        return self.bundle_data.get('sequence')
    
    def __repr__(self) -> str:
        hash_preview = self.compute_hash()[:8]
        seq = self.get_sequence_number()
        op_count = len(self.get_operations())
        return f"Bundle(seq={seq}, ops={op_count}, hash={hash_preview}...)"
