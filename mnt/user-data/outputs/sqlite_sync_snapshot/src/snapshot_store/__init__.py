"""
Snapshot Store - Content-addressed snapshot and object storage for sqlite-sync-core.

This package provides:
- Immutable content-addressed object storage
- Snapshot management with deterministic state references
- Integrity verification and tamper detection
- Garbage collection
- Integration with sqlite-sync-core

Main entry point:
    SnapshotStoreEngine - primary interface for all operations

Example usage:
    from snapshot_store import SnapshotStoreEngine
    
    engine = SnapshotStoreEngine('/path/to/store')
    engine.initialize()
    
    # Import sync bundles
    bundles, snapshot = engine.import_sync_bundles(
        sync_bundles,
        snapshot_name='main'
    )
    
    # Verify integrity
    result = engine.verify_snapshot(snapshot)
    
    # Garbage collect
    gc_result = engine.garbage_collect(dry_run=True)
"""

from .engine import SnapshotStoreEngine
from .errors import (
    SnapshotStoreError,
    ObjectNotFoundError,
    ObjectCorruptedError,
    InvalidObjectError,
    SnapshotVerificationError,
    ReferenceMissingError,
    TamperDetectedError,
    GarbageCollectionError,
    InvariantViolationError,
    StorageError,
    InvalidReferenceError,
)
from .model.blob import Blob
from .model.bundle import Bundle
from .model.snapshot import Snapshot
from .model.tree import Tree

__version__ = '0.1.0'

__all__ = [
    # Main engine
    'SnapshotStoreEngine',
    
    # Errors
    'SnapshotStoreError',
    'ObjectNotFoundError',
    'ObjectCorruptedError',
    'InvalidObjectError',
    'SnapshotVerificationError',
    'ReferenceMissingError',
    'TamperDetectedError',
    'GarbageCollectionError',
    'InvariantViolationError',
    'StorageError',
    'InvalidReferenceError',
    
    # Models
    'Blob',
    'Bundle',
    'Snapshot',
    'Tree',
]
