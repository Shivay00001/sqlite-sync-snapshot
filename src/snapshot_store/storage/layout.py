"""
Filesystem layout for object storage.

Implements content-addressed storage with directory sharding.
"""

import os
from pathlib import Path
from typing import Optional

from ..errors import StorageError
from ..integrity.hashing import get_hash_prefix


class StorageLayout:
    """
    Manages filesystem layout for content-addressed objects.
    
    Layout:
        store_root/
            objects/
                <prefix>/
                    <hash>       # object file
            snapshots/
                <name>           # named snapshot references
            refs/
                <ref_name>       # additional references (tags, etc)
    """
    
    def __init__(self, store_root: Path):
        """Initialize storage layout at given root."""
        self.store_root = Path(store_root).resolve()
        self.objects_dir = self.store_root / "objects"
        self.snapshots_dir = self.store_root / "snapshots"
        self.refs_dir = self.store_root / "refs"
    
    def initialize(self) -> None:
        """
        Initialize storage directory structure.
        
        Creates all necessary directories.
        Idempotent - safe to call multiple times.
        """
        try:
            self.store_root.mkdir(parents=True, exist_ok=True)
            self.objects_dir.mkdir(exist_ok=True)
            self.snapshots_dir.mkdir(exist_ok=True)
            self.refs_dir.mkdir(exist_ok=True)
        except OSError as e:
            raise StorageError("initialize", str(self.store_root), e)
    
    def get_object_path(self, obj_hash: str) -> Path:
        """
        Get filesystem path for an object by its hash.
        
        Uses 2-character prefix for directory sharding.
        """
        prefix = get_hash_prefix(obj_hash, 2)
        return self.objects_dir / prefix / obj_hash
    
    def get_snapshot_ref_path(self, name: str) -> Path:
        """Get path for a named snapshot reference."""
        # Sanitize name to prevent directory traversal
        safe_name = self._sanitize_name(name)
        return self.snapshots_dir / safe_name
    
    def get_ref_path(self, ref_name: str) -> Path:
        """Get path for a named reference."""
        safe_name = self._sanitize_name(ref_name)
        return self.refs_dir / safe_name
    
    def ensure_object_directory(self, obj_hash: str) -> None:
        """Ensure the directory for an object exists."""
        prefix = get_hash_prefix(obj_hash, 2)
        prefix_dir = self.objects_dir / prefix
        try:
            prefix_dir.mkdir(exist_ok=True)
        except OSError as e:
            raise StorageError("mkdir", str(prefix_dir), e)
    
    def list_all_objects(self) -> list[str]:
        """
        List all object hashes in the store.
        
        Scans all prefix directories.
        """
        objects = []
        
        if not self.objects_dir.exists():
            return objects
        
        try:
            for prefix_dir in self.objects_dir.iterdir():
                if not prefix_dir.is_dir():
                    continue
                
                for obj_file in prefix_dir.iterdir():
                    if obj_file.is_file():
                        objects.append(obj_file.name)
        
        except OSError as e:
            raise StorageError("list_objects", str(self.objects_dir), e)
        
        return objects
    
    def list_snapshot_refs(self) -> list[str]:
        """List all named snapshot references."""
        if not self.snapshots_dir.exists():
            return []
        
        try:
            return [f.name for f in self.snapshots_dir.iterdir() if f.is_file()]
        except OSError as e:
            raise StorageError("list_snapshots", str(self.snapshots_dir), e)
    
    def object_exists(self, obj_hash: str) -> bool:
        """Check if an object exists in storage."""
        return self.get_object_path(obj_hash).exists()
    
    def snapshot_ref_exists(self, name: str) -> bool:
        """Check if a named snapshot reference exists."""
        return self.get_snapshot_ref_path(name).exists()
    
    @staticmethod
    def _sanitize_name(name: str) -> str:
        """
        Sanitize a name for safe filesystem use.
        
        Prevents directory traversal and invalid characters.
        """
        # Remove any path separators
        name = name.replace('/', '_').replace('\\', '_')
        # Remove any leading dots
        name = name.lstrip('.')
        # Ensure not empty
        if not name:
            raise ValueError("Name cannot be empty after sanitization")
        return name
    
    def get_storage_stats(self) -> dict:
        """
        Get storage statistics.
        
        Returns dict with:
        - total_objects: number of objects
        - total_size_bytes: total size in bytes
        - snapshot_refs: number of named snapshots
        """
        stats = {
            'total_objects': 0,
            'total_size_bytes': 0,
            'snapshot_refs': 0,
        }
        
        # Count objects and size
        try:
            for obj_hash in self.list_all_objects():
                obj_path = self.get_object_path(obj_hash)
                if obj_path.exists():
                    stats['total_objects'] += 1
                    stats['total_size_bytes'] += obj_path.stat().st_size
        except OSError:
            pass  # Best effort
        
        # Count snapshot refs
        try:
            stats['snapshot_refs'] = len(self.list_snapshot_refs())
        except OSError:
            pass  # Best effort
        
        return stats
