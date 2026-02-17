"""
Test package structure and exports.

Verifies that the package is correctly structured and exposes the right API.
"""

import pytest
import snapshot_store
from snapshot_store import (
    SnapshotStoreEngine,
    Blob,
    Bundle,
    Snapshot,
    Tree,
    SnapshotStoreError,
)


def test_package_exports():
    """Verify that the package exposes the expected classes."""
    assert SnapshotStoreEngine is not None
    assert Blob is not None
    assert Bundle is not None
    assert Snapshot is not None
    assert Tree is not None
    assert SnapshotStoreError is not None


def test_engine_initialization(tmp_path):
    """Verify that the engine can be initialized."""
    engine = SnapshotStoreEngine(tmp_path)
    engine.initialize()
    
    assert (tmp_path / "objects").exists()
    assert (tmp_path / "snapshots").exists()
    assert (tmp_path / "refs").exists()


def test_subpackage_imports():
    """Verify that subpackages are importable (even if not exposed directly)."""
    import snapshot_store.storage.object_store
    import snapshot_store.integrity.hashing
    import snapshot_store.model.blob
    import snapshot_store.integration.sync_adapter
    
    assert snapshot_store.storage.object_store.ObjectStore is not None
    assert snapshot_store.integrity.hashing.compute_hash is not None
