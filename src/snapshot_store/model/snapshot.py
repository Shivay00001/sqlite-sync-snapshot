"""
Snapshot object model.

Snapshots represent deterministic state references.
"""

from typing import Optional, List
from ..integrity.hashing import compute_object_hash


class Snapshot:
    """
    Immutable snapshot object representing a deterministic state.
    
    A snapshot references:
    - A list of bundle hashes (ordered)
    - Optional parent snapshot hash
    - Metadata (description, timestamp, etc)
    
    Snapshots form a DAG (directed acyclic graph) through parent references.
    """
    
    def __init__(
        self,
        bundles: List[str],
        parent: Optional[str] = None,
        metadata: Optional[dict] = None,
    ):
        """
        Create a snapshot.
        
        Args:
            bundles: ordered list of bundle hashes
            parent: optional parent snapshot hash
            metadata: optional metadata
        """
        self.bundles = list(bundles)  # Copy to ensure immutability
        self.parent = parent
        self.metadata = metadata or {}
    
    def to_dict(self) -> dict:
        """
        Convert snapshot to storable dictionary representation.
        
        Returns canonical dict that can be hashed and stored.
        """
        content = {
            'bundles': self.bundles,
        }
        
        if self.parent:
            content['parent'] = self.parent
        
        obj = {
            'type': 'snapshot',
            'content': content,
        }
        
        if self.metadata:
            obj['metadata'] = self.metadata
        
        return obj
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Snapshot':
        """
        Reconstruct snapshot from stored dictionary.
        
        Raises ValueError if data is invalid.
        """
        if data.get('type') != 'snapshot':
            raise ValueError(f"Invalid snapshot type: {data.get('type')}")
        
        if 'content' not in data:
            raise ValueError("Snapshot missing content field")
        
        content = data['content']
        
        if 'bundles' not in content:
            raise ValueError("Snapshot content missing bundles field")
        
        bundles = content['bundles']
        if not isinstance(bundles, list):
            raise ValueError("Snapshot bundles must be a list")
        
        parent = content.get('parent')
        metadata = data.get('metadata', {})
        
        return cls(bundles, parent, metadata)
    
    def compute_hash(self) -> str:
        """Compute content hash of this snapshot."""
        return compute_object_hash(self.to_dict())
    
    def bundle_count(self) -> int:
        """Get number of bundles in this snapshot."""
        return len(self.bundles)
    
    def has_parent(self) -> bool:
        """Check if this snapshot has a parent."""
        return self.parent is not None
    
    def get_all_references(self) -> List[str]:
        """
        Get all object hashes referenced by this snapshot.
        
        Returns list of bundle hashes plus parent if present.
        """
        refs = list(self.bundles)
        if self.parent:
            refs.append(self.parent)
        return refs
    
    def with_parent(self, parent_hash: str) -> 'Snapshot':
        """
        Create a new snapshot with a parent reference.
        
        Returns new Snapshot instance (immutable).
        """
        return Snapshot(
            bundles=self.bundles,
            parent=parent_hash,
            metadata=self.metadata.copy(),
        )
    
    def with_additional_bundles(self, new_bundles: List[str]) -> 'Snapshot':
        """
        Create a new snapshot with additional bundles.
        
        Returns new Snapshot instance (immutable).
        """
        return Snapshot(
            bundles=self.bundles + new_bundles,
            parent=self.parent,
            metadata=self.metadata.copy(),
        )
    
    def __repr__(self) -> str:
        hash_preview = self.compute_hash()[:8]
        bundle_count = len(self.bundles)
        parent_preview = self.parent[:8] + "..." if self.parent else "None"
        return f"Snapshot(bundles={bundle_count}, parent={parent_preview}, hash={hash_preview}...)"
