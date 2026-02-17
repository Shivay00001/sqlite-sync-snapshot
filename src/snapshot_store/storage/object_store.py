"""
Content-addressed object storage.

Provides immutable object storage with content addressing.
"""

import json
from pathlib import Path
from typing import Optional, Set
import os
import tempfile

from ..errors import (
    ObjectNotFoundError,
    ObjectCorruptedError,
    StorageError,
    InvalidObjectError,
)
from ..integrity.hashing import compute_hash, compute_object_hash
from ..integrity.canonical import canonical_json
from ..integrity.verification import (
    verify_object_integrity,
    verify_object_structure,
)
from .layout import StorageLayout


class ObjectStore:
    """
    Content-addressed object store with immutable objects.
    
    Objects are stored by their content hash.
    Once written, objects never change.
    """
    
    def __init__(self, layout: StorageLayout):
        """Initialize object store with given layout."""
        self.layout = layout
    
    def put_object(self, obj_data: dict) -> str:
        """
        Store an object and return its hash.
        
        The object is stored immutably:
        - Hash is computed from canonical representation
        - Object is written atomically
        - If hash already exists, no action (idempotent)
        
        Returns the content hash.
        """
        # Verify object structure
        verify_object_structure(obj_data)
        
        # Compute hash from canonical representation
        obj_hash = compute_object_hash(obj_data)
        
        # Check if already exists (idempotent)
        obj_path = self.layout.get_object_path(obj_hash)
        if obj_path.exists():
            # Verify existing object integrity
            existing_data = self._read_object_file(obj_path)
            try:
                existing_obj = json.loads(existing_data.decode('utf-8'))
                verify_object_integrity(existing_obj, obj_hash)
                return obj_hash  # Already exists and valid
            except (json.JSONDecodeError, ObjectCorruptedError):
                # Existing file is corrupted, will overwrite
                pass
        
        # Ensure directory exists
        self.layout.ensure_object_directory(obj_hash)
        
        # Write atomically using temp file + rename
        canonical_bytes = canonical_json(obj_data)
        self._write_object_atomic(obj_path, canonical_bytes)
        
        return obj_hash
    
    def get_object(self, obj_hash: str, verify: bool = True) -> dict:
        """
        Retrieve an object by its hash.
        
        If verify=True (default), verifies integrity before returning.
        
        Raises ObjectNotFoundError if object doesn't exist.
        Raises ObjectCorruptedError if verification fails.
        """
        obj_path = self.layout.get_object_path(obj_hash)
        
        if not obj_path.exists():
            raise ObjectNotFoundError(obj_hash)
        
        try:
            data = self._read_object_file(obj_path)
            obj_data = json.loads(data.decode('utf-8'))
        except (OSError, json.JSONDecodeError) as e:
            raise StorageError("read_object", str(obj_path), e)
        
        if verify:
            verify_object_structure(obj_data)
            verify_object_integrity(obj_data, obj_hash)
        
        return obj_data
    
    def has_object(self, obj_hash: str) -> bool:
        """Check if an object exists in the store."""
        return self.layout.object_exists(obj_hash)
    
    def delete_object(self, obj_hash: str) -> bool:
        """
        Delete an object from the store.
        
        This is used by garbage collection.
        Use with extreme caution - only delete unreachable objects.
        
        Returns True if deleted, False if didn't exist.
        """
        obj_path = self.layout.get_object_path(obj_hash)
        
        if not obj_path.exists():
            return False
        
        try:
            obj_path.unlink()
            return True
        except OSError as e:
            raise StorageError("delete_object", str(obj_path), e)
    
    def list_all_objects(self) -> list[str]:
        """List all object hashes in the store."""
        return self.layout.list_all_objects()
    
    def put_snapshot_ref(self, name: str, snapshot_hash: str) -> None:
        """
        Create a named reference to a snapshot.
        
        This creates a GC root - the snapshot and all its references
        will be protected from garbage collection.
        """
        # Verify snapshot exists
        if not self.has_object(snapshot_hash):
            raise ObjectNotFoundError(snapshot_hash)
        
        ref_path = self.layout.get_snapshot_ref_path(name)
        
        try:
            ref_path.parent.mkdir(parents=True, exist_ok=True)
            ref_path.write_text(snapshot_hash, encoding='utf-8')
        except OSError as e:
            raise StorageError("write_snapshot_ref", str(ref_path), e)
    
    def get_snapshot_ref(self, name: str) -> Optional[str]:
        """
        Get the snapshot hash for a named reference.
        
        Returns None if reference doesn't exist.
        """
        ref_path = self.layout.get_snapshot_ref_path(name)
        
        if not ref_path.exists():
            return None
        
        try:
            return ref_path.read_text(encoding='utf-8').strip()
        except OSError as e:
            raise StorageError("read_snapshot_ref", str(ref_path), e)
    
    def delete_snapshot_ref(self, name: str) -> bool:
        """
        Delete a named snapshot reference.
        
        Returns True if deleted, False if didn't exist.
        """
        ref_path = self.layout.get_snapshot_ref_path(name)
        
        if not ref_path.exists():
            return False
        
        try:
            ref_path.unlink()
            return True
        except OSError as e:
            raise StorageError("delete_snapshot_ref", str(ref_path), e)
    
    def list_snapshot_refs(self) -> list[str]:
        """List all named snapshot references."""
        return self.layout.list_snapshot_refs()
    
    def _read_object_file(self, path: Path) -> bytes:
        """Read object file contents."""
        try:
            return path.read_bytes()
        except OSError as e:
            raise StorageError("read_file", str(path), e)
    
    def _write_object_atomic(self, path: Path, data: bytes) -> None:
        """
        Write object file atomically.
        
        Uses temp file + rename for atomicity.
        """
        dir_path = path.parent
        fd = None
        temp_path = None
        try:
            # Write to temporary file in same directory
            fd, temp_path = tempfile.mkstemp(
                dir=str(dir_path),
                prefix='.tmp_',
                suffix='.json'
            )
            
            # Write data and close fd
            os.write(fd, data)
            os.close(fd)
            fd = None  # Mark as closed so we don't double-close
            
            # Atomic replace (works cross-platform including Windows)
            os.replace(temp_path, path)
            temp_path = None  # Mark as moved so we don't try to clean up
        
        except Exception as e:
            # Clean up fd if still open
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            # Clean up temp file if still exists
            if temp_path is not None and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass
            if isinstance(e, OSError):
                raise StorageError("write_file", str(path), e)
            raise
    
    def get_stats(self) -> dict:
        """Get storage statistics."""
        return self.layout.get_storage_stats()
