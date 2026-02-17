"""
Integration adapter for sqlite-sync-core.

Provides seamless conversion between sync bundles and snapshot objects.
"""

from typing import List, Optional, Dict, Any
from ..model.bundle import Bundle
from ..model.snapshot import Snapshot
from ..errors import InvalidObjectError


class SyncAdapter:
    """
    Adapter for integrating sqlite-sync-core bundles into the snapshot store.
    
    This adapter:
    - Converts sync bundles to content-addressed bundle objects
    - Creates snapshots from sequences of bundles
    - Maintains the integrity of sqlite-sync-core's operation log
    - Does NOT modify sqlite-sync-core behavior
    """
    
    def __init__(self, object_store):
        """
        Initialize adapter with an object store.
        
        Args:
            object_store: ObjectStore instance for storing objects
        """
        self.store = object_store
    
    def import_bundle(
        self,
        bundle_data: dict,
        metadata: Optional[dict] = None
    ) -> str:
        """
        Import a sync bundle from sqlite-sync-core.
        
        The bundle is stored as a content-addressed object.
        Returns the bundle hash.
        
        Args:
            bundle_data: sync bundle from sqlite-sync-core
            metadata: optional metadata (source, timestamp, etc)
        
        Returns:
            str: hash of the stored bundle
        """
        # Validate bundle has expected structure
        self._validate_bundle(bundle_data)
        
        # Create bundle object
        bundle = Bundle(bundle_data, metadata)
        
        # Store in object store
        bundle_hash = self.store.put_object(bundle.to_dict())
        
        return bundle_hash
    
    def import_bundles(
        self,
        bundles: List[dict],
        metadata_func: Optional[callable] = None
    ) -> List[str]:
        """
        Import multiple sync bundles.
        
        Returns list of bundle hashes in same order as input.
        
        Args:
            bundles: list of sync bundles
            metadata_func: optional function to generate metadata for each bundle
        
        Returns:
            list[str]: list of bundle hashes
        """
        bundle_hashes = []
        
        for i, bundle_data in enumerate(bundles):
            metadata = metadata_func(bundle_data, i) if metadata_func else None
            bundle_hash = self.import_bundle(bundle_data, metadata)
            bundle_hashes.append(bundle_hash)
        
        return bundle_hashes
    
    def create_snapshot_from_bundles(
        self,
        bundle_hashes: List[str],
        parent: Optional[str] = None,
        metadata: Optional[dict] = None
    ) -> str:
        """
        Create a snapshot referencing multiple bundles.
        
        The snapshot represents a deterministic state composed of
        the ordered sequence of bundles.
        
        Args:
            bundle_hashes: ordered list of bundle hashes
            parent: optional parent snapshot hash
            metadata: optional snapshot metadata
        
        Returns:
            str: hash of the created snapshot
        """
        # Verify all bundles exist
        for bundle_hash in bundle_hashes:
            if not self.store.has_object(bundle_hash):
                raise InvalidObjectError(
                    f"Bundle does not exist: {bundle_hash}",
                    bundle_hash
                )
        
        # Create snapshot
        snapshot = Snapshot(bundle_hashes, parent, metadata)
        
        # Store in object store
        snapshot_hash = self.store.put_object(snapshot.to_dict())
        
        return snapshot_hash
    
    def import_and_snapshot(
        self,
        bundles: List[dict],
        parent: Optional[str] = None,
        snapshot_name: Optional[str] = None,
        metadata: Optional[dict] = None
    ) -> tuple[List[str], str]:
        """
        Import bundles and create a snapshot in one operation.
        
        This is the main workflow for integrating sqlite-sync-core output:
        1. Import all bundles as content-addressed objects
        2. Create snapshot referencing those bundles
        3. Optionally create named reference to snapshot
        
        Args:
            bundles: list of sync bundles from sqlite-sync-core
            parent: optional parent snapshot hash
            snapshot_name: optional name for snapshot reference
            metadata: optional snapshot metadata
        
        Returns:
            tuple: (bundle_hashes, snapshot_hash)
        """
        # Import all bundles
        bundle_hashes = self.import_bundles(bundles)
        
        # Create snapshot
        snapshot_hash = self.create_snapshot_from_bundles(
            bundle_hashes,
            parent,
            metadata
        )
        
        # Create named reference if requested
        if snapshot_name:
            self.store.put_snapshot_ref(snapshot_name, snapshot_hash)
        
        return bundle_hashes, snapshot_hash
    
    def extend_snapshot(
        self,
        parent_hash: str,
        new_bundles: List[dict],
        snapshot_name: Optional[str] = None,
        metadata: Optional[dict] = None
    ) -> tuple[List[str], str]:
        """
        Extend an existing snapshot with new bundles.
        
        This creates a new snapshot that references the parent snapshot
        and includes additional bundles.
        
        Args:
            parent_hash: hash of parent snapshot to extend
            new_bundles: list of new sync bundles
            snapshot_name: optional name for new snapshot
            metadata: optional metadata for new snapshot
        
        Returns:
            tuple: (new_bundle_hashes, new_snapshot_hash)
        """
        # Verify parent exists
        if not self.store.has_object(parent_hash):
            raise InvalidObjectError(
                f"Parent snapshot does not exist: {parent_hash}",
                parent_hash
            )
        
        # Import new bundles and create snapshot
        return self.import_and_snapshot(
            new_bundles,
            parent=parent_hash,
            snapshot_name=snapshot_name,
            metadata=metadata
        )
    
    def export_bundle(self, bundle_hash: str) -> dict:
        """
        Export a bundle object back to sqlite-sync-core format.
        
        Args:
            bundle_hash: hash of bundle to export
        
        Returns:
            dict: original sync bundle data
        """
        obj_data = self.store.get_object(bundle_hash)
        bundle = Bundle.from_dict(obj_data)
        return bundle.bundle_data
    
    def export_snapshot_bundles(self, snapshot_hash: str) -> List[dict]:
        """
        Export all bundles from a snapshot.
        
        Returns bundles in the order they appear in the snapshot.
        
        Args:
            snapshot_hash: hash of snapshot to export
        
        Returns:
            list[dict]: list of sync bundle data
        """
        # Load snapshot
        obj_data = self.store.get_object(snapshot_hash)
        snapshot = Snapshot.from_dict(obj_data)
        
        # Export all bundles
        bundles = []
        for bundle_hash in snapshot.bundles:
            bundle_data = self.export_bundle(bundle_hash)
            bundles.append(bundle_data)
        
        return bundles
    
    def get_snapshot_chain(self, snapshot_hash: str) -> List[str]:
        """
        Get the chain of snapshots from root to given snapshot.
        
        Walks parent references back to find the full chain.
        
        Args:
            snapshot_hash: hash of snapshot to start from
        
        Returns:
            list[str]: list of snapshot hashes from root to current
        """
        chain = []
        current = snapshot_hash
        visited = set()  # Detect cycles
        
        while current:
            if current in visited:
                raise InvalidObjectError(
                    f"Cycle detected in snapshot chain at {current}"
                )
            
            visited.add(current)
            chain.append(current)
            
            # Load snapshot and get parent
            obj_data = self.store.get_object(current)
            snapshot = Snapshot.from_dict(obj_data)
            current = snapshot.parent
        
        # Reverse to get root-first order
        return list(reversed(chain))
    
    def _validate_bundle(self, bundle_data: dict) -> None:
        """
        Validate that bundle data has expected structure.
        
        This is a basic check - we don't enforce sqlite-sync-core's
        exact schema since that may evolve.
        """
        if not isinstance(bundle_data, dict):
            raise InvalidObjectError("Bundle must be a dictionary")
        
        # Bundle should have some recognizable fields
        # but we're permissive about exact structure
        if not bundle_data:
            raise InvalidObjectError("Bundle cannot be empty")
    
    def get_statistics(self) -> dict:
        """
        Get statistics about stored bundles and snapshots.
        
        Returns:
            dict: statistics including counts and sizes
        """
        stats = self.store.get_stats()
        
        # Count bundles and snapshots
        bundle_count = 0
        snapshot_count = 0
        
        for obj_hash in self.store.list_all_objects():
            try:
                obj_data = self.store.get_object(obj_hash, verify=False)
                obj_type = obj_data.get('type')
                
                if obj_type == 'bundle':
                    bundle_count += 1
                elif obj_type == 'snapshot':
                    snapshot_count += 1
            except Exception:
                continue  # Skip corrupted objects
        
        stats['bundle_count'] = bundle_count
        stats['snapshot_count'] = snapshot_count
        
        return stats
