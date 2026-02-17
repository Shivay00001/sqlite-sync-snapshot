"""
Snapshot Store Engine.

Main entry point coordinating all components.
"""

from pathlib import Path
from typing import Optional, List, Set, Dict
import json

from .storage.layout import StorageLayout
from .storage.object_store import ObjectStore
from .storage.gc import GarbageCollector
from .integrity.verification import (
    verify_snapshot_recursive,
    detect_tampering,
)
from .integration.sync_adapter import SyncAdapter
from .errors import (
    SnapshotStoreError,
    SnapshotVerificationError,
    TamperDetectedError,
)
from .model.blob import Blob
from .model.bundle import Bundle
from .model.snapshot import Snapshot
from .model.tree import Tree


class SnapshotStoreEngine:
    """
    Main engine for snapshot and object store operations.
    
    This is the primary interface for:
    - Storing objects (blobs, bundles, snapshots, trees)
    - Retrieving and verifying objects
    - Managing snapshots
    - Running garbage collection
    - Integrating with sqlite-sync-core
    """
    
    def __init__(self, store_path: str | Path):
        """
        Initialize snapshot store at given path.
        
        Args:
            store_path: filesystem path for object storage
        """
        self.store_path = Path(store_path).resolve()
        self.layout = StorageLayout(self.store_path)
        self.object_store = ObjectStore(self.layout)
        self.sync_adapter = SyncAdapter(self.object_store)
        
        # Initialize garbage collector
        self.gc = GarbageCollector(
            list_all_func=self.object_store.list_all_objects,
            load_object_func=lambda h: self.object_store.get_object(h, verify=False),
            delete_object_func=self.object_store.delete_object,
            exists_func=self.object_store.has_object,
        )
    
    def initialize(self) -> None:
        """
        Initialize the store.
        
        Creates necessary directory structure.
        Safe to call multiple times (idempotent).
        """
        self.layout.initialize()
    
    # ========== Object Storage ==========
    
    def put_blob(self, data: bytes, metadata: Optional[dict] = None) -> str:
        """
        Store a blob and return its hash.
        
        Args:
            data: raw binary data
            metadata: optional metadata
        
        Returns:
            str: content hash of stored blob
        """
        blob = Blob(data, metadata)
        return self.object_store.put_object(blob.to_dict())
    
    def get_blob(self, blob_hash: str) -> Blob:
        """Retrieve a blob by hash."""
        obj_data = self.object_store.get_object(blob_hash)
        return Blob.from_dict(obj_data)
    
    def put_bundle(self, bundle_data: dict, metadata: Optional[dict] = None) -> str:
        """
        Store a bundle and return its hash.
        
        Args:
            bundle_data: sync bundle from sqlite-sync-core
            metadata: optional metadata
        
        Returns:
            str: content hash of stored bundle
        """
        return self.sync_adapter.import_bundle(bundle_data, metadata)
    
    def get_bundle(self, bundle_hash: str) -> Bundle:
        """Retrieve a bundle by hash."""
        obj_data = self.object_store.get_object(bundle_hash)
        return Bundle.from_dict(obj_data)
    
    def put_snapshot(
        self,
        bundles: List[str],
        parent: Optional[str] = None,
        metadata: Optional[dict] = None
    ) -> str:
        """
        Create a snapshot and return its hash.
        
        Args:
            bundles: ordered list of bundle hashes
            parent: optional parent snapshot hash
            metadata: optional metadata
        
        Returns:
            str: content hash of stored snapshot
        """
        snapshot = Snapshot(bundles, parent, metadata)
        return self.object_store.put_object(snapshot.to_dict())
    
    def get_snapshot(self, snapshot_hash: str) -> Snapshot:
        """Retrieve a snapshot by hash."""
        obj_data = self.object_store.get_object(snapshot_hash)
        return Snapshot.from_dict(obj_data)
    
    def put_tree(self, children: List[str], metadata: Optional[dict] = None) -> str:
        """
        Create a tree and return its hash.
        
        Args:
            children: list of child object hashes
            metadata: optional metadata
        
        Returns:
            str: content hash of stored tree
        """
        tree = Tree(children, metadata)
        return self.object_store.put_object(tree.to_dict())
    
    def get_tree(self, tree_hash: str) -> Tree:
        """Retrieve a tree by hash."""
        obj_data = self.object_store.get_object(tree_hash)
        return Tree.from_dict(obj_data)
    
    def has_object(self, obj_hash: str) -> bool:
        """Check if an object exists."""
        return self.object_store.has_object(obj_hash)
    
    def get_object_raw(self, obj_hash: str) -> dict:
        """Get raw object dictionary."""
        return self.object_store.get_object(obj_hash)
    
    # ========== Named References ==========
    
    def create_snapshot_ref(self, name: str, snapshot_hash: str) -> None:
        """
        Create a named reference to a snapshot.
        
        This creates a GC root - the snapshot will be protected.
        """
        self.object_store.put_snapshot_ref(name, snapshot_hash)
    
    def get_snapshot_ref(self, name: str) -> Optional[str]:
        """Get snapshot hash for a named reference."""
        return self.object_store.get_snapshot_ref(name)
    
    def delete_snapshot_ref(self, name: str) -> bool:
        """Delete a named snapshot reference."""
        return self.object_store.delete_snapshot_ref(name)
    
    def list_snapshot_refs(self) -> List[str]:
        """List all named snapshot references."""
        return self.object_store.list_snapshot_refs()
    
    # ========== Integrity Verification ==========
    
    def verify_object(self, obj_hash: str) -> bool:
        """
        Verify an object's integrity.
        
        Returns True if valid.
        Raises ObjectCorruptedError if corrupted.
        """
        # get_object with verify=True will check integrity
        self.object_store.get_object(obj_hash, verify=True)
        return True
    
    def verify_snapshot(self, snapshot_hash: str) -> Dict[str, any]:
        """
        Verify a snapshot and all its references recursively.
        
        Returns dict with:
            - valid: bool
            - errors: list of error messages
        """
        is_valid, errors = verify_snapshot_recursive(
            snapshot_hash,
            load_func=lambda h: self.object_store.get_object(h, verify=False),
            exists_func=self.object_store.has_object,
        )
        
        return {
            'valid': is_valid,
            'errors': errors,
        }
    
    def detect_tampering(self) -> Dict[str, any]:
        """
        Detect tampering across all stored objects.
        
        Verifies that all objects' content matches their hashes.
        
        Returns dict with:
            - tampered: list of tampered object hashes
            - verified: count of verified objects
            - errors: list of errors encountered
        """
        result = {
            'tampered': [],
            'verified': 0,
            'errors': [],
        }
        
        for obj_hash in self.object_store.list_all_objects():
            try:
                self.verify_object(obj_hash)
                result['verified'] += 1
            except Exception as e:
                result['tampered'].append(obj_hash)
                result['errors'].append(f"{obj_hash}: {e}")
        
        return result
    
    def detect_missing_objects(self) -> Dict[str, any]:
        """
        Detect snapshots with missing referenced objects.
        
        Returns dict with:
            - broken_snapshots: list of snapshot hashes with missing refs
            - missing_objects: set of missing object hashes
        """
        result = {
            'broken_snapshots': [],
            'missing_objects': set(),
        }
        
        # Check all snapshots
        for obj_hash in self.object_store.list_all_objects():
            try:
                obj_data = self.object_store.get_object(obj_hash, verify=False)
                
                if obj_data.get('type') != 'snapshot':
                    continue
                
                # Verify this snapshot
                verify_result = self.verify_snapshot(obj_hash)
                
                if not verify_result['valid']:
                    result['broken_snapshots'].append(obj_hash)
                    
                    # Extract missing objects from errors
                    for error in verify_result['errors']:
                        if 'missing' in error.lower():
                            # Try to extract hash from error message
                            parts = error.split()
                            for part in parts:
                                if len(part) == 64:  # BLAKE3 hash length
                                    result['missing_objects'].add(part)
            
            except Exception:
                continue
        
        return result
    
    # ========== Garbage Collection ==========
    
    def garbage_collect(self, dry_run: bool = False) -> Dict[str, any]:
        """
        Run garbage collection.
        
        Deletes unreachable objects not referenced by any named snapshot.
        
        Args:
            dry_run: if True, only report what would be deleted
        
        Returns dict with GC results.
        """
        # Find all roots from named snapshot references
        roots = set()
        
        for ref_name in self.list_snapshot_refs():
            snapshot_hash = self.get_snapshot_ref(ref_name)
            if snapshot_hash:
                roots.add(snapshot_hash)
        
        # Run garbage collection
        return self.gc.collect(roots, dry_run=dry_run)
    
    def verify_gc_safety(self) -> List[str]:
        """
        Verify that garbage collection would be safe.
        
        Returns list of warnings/issues.
        """
        roots = set()
        
        for ref_name in self.list_snapshot_refs():
            snapshot_hash = self.get_snapshot_ref(ref_name)
            if snapshot_hash:
                roots.add(snapshot_hash)
        
        return self.gc.verify_gc_safety(roots)
    
    # ========== Integration with sqlite-sync-core ==========
    
    def import_sync_bundles(
        self,
        bundles: List[dict],
        parent: Optional[str] = None,
        snapshot_name: Optional[str] = None,
        metadata: Optional[dict] = None
    ) -> tuple[List[str], str]:
        """
        Import sync bundles and create snapshot.
        
        This is the main integration point for sqlite-sync-core.
        
        Args:
            bundles: list of sync bundles
            parent: optional parent snapshot
            snapshot_name: optional name for snapshot
            metadata: optional metadata
        
        Returns:
            tuple: (bundle_hashes, snapshot_hash)
        """
        return self.sync_adapter.import_and_snapshot(
            bundles,
            parent,
            snapshot_name,
            metadata
        )
    
    def extend_snapshot(
        self,
        parent_hash: str,
        new_bundles: List[dict],
        snapshot_name: Optional[str] = None,
        metadata: Optional[dict] = None
    ) -> tuple[List[str], str]:
        """
        Extend an existing snapshot with new bundles.
        
        Creates a new snapshot referencing the parent.
        """
        return self.sync_adapter.extend_snapshot(
            parent_hash,
            new_bundles,
            snapshot_name,
            metadata
        )
    
    def export_snapshot_bundles(self, snapshot_hash: str) -> List[dict]:
        """Export all bundles from a snapshot back to sqlite-sync-core format."""
        return self.sync_adapter.export_snapshot_bundles(snapshot_hash)
    
    # ========== Statistics and Diagnostics ==========
    
    def get_statistics(self) -> Dict[str, any]:
        """
        Get store statistics.
        
        Returns comprehensive statistics about the store.
        """
        return self.sync_adapter.get_statistics()
    
    def list_all_objects(self) -> List[str]:
        """List all object hashes in store."""
        return self.object_store.list_all_objects()
    
    def export_snapshot_json(self, snapshot_hash: str, output_path: str | Path) -> None:
        """
        Export a snapshot and all its data to a JSON file.
        
        Useful for debugging and archival.
        """
        output_path = Path(output_path)
        
        # Load snapshot
        snapshot = self.get_snapshot(snapshot_hash)
        
        # Collect all data
        export_data = {
            'snapshot_hash': snapshot_hash,
            'snapshot': snapshot.to_dict(),
            'bundles': {},
        }
        
        # Export all bundles
        for bundle_hash in snapshot.bundles:
            bundle = self.get_bundle(bundle_hash)
            export_data['bundles'][bundle_hash] = bundle.to_dict()
        
        # Write to file
        with output_path.open('w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, sort_keys=True)
    
    def __repr__(self) -> str:
        stats = self.get_statistics()
        return (
            f"SnapshotStoreEngine("
            f"path={self.store_path}, "
            f"objects={stats.get('total_objects', 0)}, "
            f"snapshots={stats.get('snapshot_refs', 0)})"
        )
