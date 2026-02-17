"""
Tree object model.

Trees provide hierarchical grouping of objects.
"""

from typing import List, Optional
from ..integrity.hashing import compute_object_hash


class Tree:
    """
    Immutable tree object for grouping other objects.
    
    Trees can reference:
    - Other trees (for hierarchical structure)
    - Blobs
    - Bundles
    - Snapshots
    
    Trees enable organizing objects into directories or collections.
    """
    
    def __init__(
        self,
        children: List[str],
        metadata: Optional[dict] = None,
    ):
        """
        Create a tree.
        
        Args:
            children: list of child object hashes
            metadata: optional metadata (names, permissions, etc)
        """
        self.children = list(children)  # Copy to ensure immutability
        self.metadata = metadata or {}
    
    def to_dict(self) -> dict:
        """
        Convert tree to storable dictionary representation.
        
        Returns canonical dict that can be hashed and stored.
        """
        content = {
            'children': self.children,
        }
        
        obj = {
            'type': 'tree',
            'content': content,
        }
        
        if self.metadata:
            obj['metadata'] = self.metadata
        
        return obj
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Tree':
        """
        Reconstruct tree from stored dictionary.
        
        Raises ValueError if data is invalid.
        """
        if data.get('type') != 'tree':
            raise ValueError(f"Invalid tree type: {data.get('type')}")
        
        if 'content' not in data:
            raise ValueError("Tree missing content field")
        
        content = data['content']
        
        if 'children' not in content:
            raise ValueError("Tree content missing children field")
        
        children = content['children']
        if not isinstance(children, list):
            raise ValueError("Tree children must be a list")
        
        metadata = data.get('metadata', {})
        
        return cls(children, metadata)
    
    def compute_hash(self) -> str:
        """Compute content hash of this tree."""
        return compute_object_hash(self.to_dict())
    
    def child_count(self) -> int:
        """Get number of children in this tree."""
        return len(self.children)
    
    def has_children(self) -> bool:
        """Check if this tree has any children."""
        return len(self.children) > 0
    
    def get_child_names(self) -> dict:
        """
        Get mapping of child hashes to names if present in metadata.
        
        Returns dict mapping hash -> name.
        """
        names = self.metadata.get('names', {})
        return {child: names.get(child, '') for child in self.children}
    
    def with_child(self, child_hash: str, name: Optional[str] = None) -> 'Tree':
        """
        Create a new tree with an additional child.
        
        Returns new Tree instance (immutable).
        """
        new_children = self.children + [child_hash]
        new_metadata = self.metadata.copy()
        
        if name:
            if 'names' not in new_metadata:
                new_metadata['names'] = {}
            new_metadata['names'][child_hash] = name
        
        return Tree(new_children, new_metadata)
    
    def without_child(self, child_hash: str) -> 'Tree':
        """
        Create a new tree without a specific child.
        
        Returns new Tree instance (immutable).
        """
        new_children = [c for c in self.children if c != child_hash]
        new_metadata = self.metadata.copy()
        
        # Remove name if present
        if 'names' in new_metadata and child_hash in new_metadata['names']:
            new_metadata['names'] = {
                k: v for k, v in new_metadata['names'].items()
                if k != child_hash
            }
        
        return Tree(new_children, new_metadata)
    
    def __repr__(self) -> str:
        hash_preview = self.compute_hash()[:8]
        child_count = len(self.children)
        return f"Tree(children={child_count}, hash={hash_preview}...)"
