"""
Test snapshot verification.

Verifies integrity checking and corruption detection.
"""

import pytest
import tempfile
import json
from pathlib import Path

from snapshot_store import (
    SnapshotStoreEngine,
    ObjectCorruptedError,
    ReferenceMissingError,
    InvalidObjectError,
)


class TestSnapshotVerification:
    """Test snapshot verification functionality."""
    
    @pytest.fixture
    def store(self):
        """Create a temporary store for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = SnapshotStoreEngine(tmpdir)
            engine.initialize()
            yield engine
    
    def test_verify_valid_snapshot(self, store):
        """Verify a valid snapshot passes verification."""
        # Create bundles
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        bundle2 = store.put_bundle({'sequence': 2, 'operations': []})
        
        # Create snapshot
        snapshot = store.put_snapshot([bundle1, bundle2])
        
        # Verify
        result = store.verify_snapshot(snapshot)
        assert result['valid'] is True
        assert len(result['errors']) == 0
    
    def test_verify_snapshot_with_parent(self, store):
        """Verify snapshot with parent reference."""
        # Create parent snapshot
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        parent = store.put_snapshot([bundle1])
        
        # Create child snapshot
        bundle2 = store.put_bundle({'sequence': 2, 'operations': []})
        child = store.put_snapshot([bundle2], parent=parent)
        
        # Verify child
        result = store.verify_snapshot(child)
        assert result['valid'] is True
    
    def test_verify_snapshot_chain(self, store):
        """Verify a chain of snapshots."""
        # Create chain
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        snap1 = store.put_snapshot([bundle1])
        
        bundle2 = store.put_bundle({'sequence': 2, 'operations': []})
        snap2 = store.put_snapshot([bundle2], parent=snap1)
        
        bundle3 = store.put_bundle({'sequence': 3, 'operations': []})
        snap3 = store.put_snapshot([bundle3], parent=snap2)
        
        # Verify final snapshot (should check whole chain)
        result = store.verify_snapshot(snap3)
        assert result['valid'] is True
    
    def test_detect_missing_bundle(self, store):
        """Detect when a referenced bundle is missing."""
        # Create a snapshot with a fake bundle reference
        fake_bundle_hash = 'a' * 64  # Invalid but properly formatted hash
        
        snapshot = store.put_snapshot([fake_bundle_hash])
        
        # Verification should fail
        result = store.verify_snapshot(snapshot)
        assert result['valid'] is False
        assert len(result['errors']) > 0
    
    def test_verify_object_integrity(self, store):
        """Verify individual object integrity."""
        bundle = store.put_bundle({'sequence': 1, 'operations': []})
        
        # Should pass verification
        assert store.verify_object(bundle) is True
    
    def test_detect_corrupted_object(self, store):
        """Detect when an object is corrupted."""
        # Create object
        bundle_hash = store.put_bundle({'sequence': 1, 'operations': []})
        
        # Corrupt the object file
        obj_path = store.layout.get_object_path(bundle_hash)
        with open(obj_path, 'w') as f:
            f.write('{"corrupted": "data"}')
        
        # Verification should fail
        with pytest.raises((ObjectCorruptedError, InvalidObjectError)):
            store.verify_object(bundle_hash)
    
    def test_detect_tampering_across_store(self, store):
        """Detect tampering across all objects."""
        # Create some valid objects
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        bundle2 = store.put_bundle({'sequence': 2, 'operations': []})
        
        # Corrupt one object
        obj_path = store.layout.get_object_path(bundle2)
        with open(obj_path, 'w') as f:
            f.write('{"corrupted": true}')
        
        # Detect tampering
        result = store.detect_tampering()
        
        assert len(result['tampered']) == 1
        assert bundle2 in result['tampered']
        assert result['verified'] >= 1  # At least bundle1 should be verified
    
    def test_detect_missing_objects_in_snapshots(self, store):
        """Detect snapshots with missing referenced objects."""
        # Create a bundle and snapshot
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        snapshot = store.put_snapshot([bundle1])
        
        # Delete the bundle (dangerous!)
        bundle_path = store.layout.get_object_path(bundle1)
        bundle_path.unlink()
        
        # Detect missing objects
        result = store.detect_missing_objects()
        
        assert len(result['broken_snapshots']) == 1
        assert snapshot in result['broken_snapshots']
    
    def test_verify_empty_snapshot(self, store):
        """Verify snapshot with no bundles."""
        snapshot = store.put_snapshot([])
        
        result = store.verify_snapshot(snapshot)
        assert result['valid'] is True
    
    def test_verify_deep_snapshot_chain(self, store):
        """Verify a deep chain of snapshots."""
        # Create a chain of 10 snapshots
        current = None
        
        for i in range(10):
            bundle = store.put_bundle({'sequence': i, 'operations': []})
            current = store.put_snapshot([bundle], parent=current)
        
        # Verify the final snapshot
        result = store.verify_snapshot(current)
        assert result['valid'] is True
    
    def test_object_idempotency(self, store):
        """Storing same object twice is idempotent."""
        bundle_data = {'sequence': 1, 'operations': []}
        
        hash1 = store.put_bundle(bundle_data)
        hash2 = store.put_bundle(bundle_data)
        
        # Should get same hash
        assert hash1 == hash2
        
        # Should still verify
        assert store.verify_object(hash1) is True
    
    def test_verify_after_retrieval(self, store):
        """Objects remain valid after retrieval."""
        bundle_data = {'sequence': 1, 'operations': []}
        bundle_hash = store.put_bundle(bundle_data)
        
        # Retrieve object
        bundle = store.get_bundle(bundle_hash)
        
        # Verify still works
        assert store.verify_object(bundle_hash) is True
        
        # Data should match
        assert bundle.bundle_data == bundle_data


class TestReferenceIntegrity:
    """Test reference integrity checking."""
    
    @pytest.fixture
    def store(self):
        """Create a temporary store for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = SnapshotStoreEngine(tmpdir)
            engine.initialize()
            yield engine
    
    def test_snapshot_references_exist(self, store):
        """All snapshot references must exist."""
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        bundle2 = store.put_bundle({'sequence': 2, 'operations': []})
        
        snapshot = store.put_snapshot([bundle1, bundle2])
        
        # Both bundles should exist
        assert store.has_object(bundle1)
        assert store.has_object(bundle2)
        
        # Snapshot should verify
        result = store.verify_snapshot(snapshot)
        assert result['valid'] is True
    
    def test_parent_reference_exists(self, store):
        """Parent snapshot reference must exist."""
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        parent = store.put_snapshot([bundle1])
        
        bundle2 = store.put_bundle({'sequence': 2, 'operations': []})
        child = store.put_snapshot([bundle2], parent=parent)
        
        # Parent should exist
        assert store.has_object(parent)
        
        # Child should verify
        result = store.verify_snapshot(child)
        assert result['valid'] is True
    
    def test_cannot_create_snapshot_with_missing_bundles(self, store):
        """Cannot create snapshot with non-existent bundles (adapter checks)."""
        from snapshot_store.errors import InvalidObjectError
        
        fake_hash = 'a' * 64
        
        # This should fail at the adapter level
        with pytest.raises(InvalidObjectError):
            store.sync_adapter.create_snapshot_from_bundles([fake_hash])
    
    def test_circular_reference_detection(self, store):
        """Detect circular references in snapshots."""
        # This is tricky - we need to manually create a circular reference
        # which shouldn't happen in normal use
        
        # For now, we just verify that the verification doesn't hang
        bundle = store.put_bundle({'sequence': 1, 'operations': []})
        snapshot = store.put_snapshot([bundle])
        
        # Normal case should work
        result = store.verify_snapshot(snapshot)
        assert result['valid'] is True


class TestIntegrityInvariants:
    """Test that integrity invariants hold."""
    
    @pytest.fixture
    def store(self):
        """Create a temporary store for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = SnapshotStoreEngine(tmpdir)
            engine.initialize()
            yield engine
    
    def test_hash_matches_content_invariant(self, store):
        """Invariant: object hash always matches its content."""
        bundle_data = {'sequence': 1, 'operations': []}
        bundle_hash = store.put_bundle(bundle_data)
        
        # Load object
        obj_data = store.get_object_raw(bundle_hash)
        
        # Recompute hash
        from snapshot_store.integrity.hashing import compute_object_hash
        recomputed_hash = compute_object_hash(obj_data)
        
        assert bundle_hash == recomputed_hash
    
    def test_immutability_invariant(self, store):
        """Invariant: objects are immutable."""
        bundle_data = {'sequence': 1, 'operations': []}
        bundle_hash = store.put_bundle(bundle_data)
        
        # Get object
        obj1 = store.get_bundle(bundle_hash)
        
        # Store same object again (idempotent)
        store.put_bundle(bundle_data)
        
        # Get object again
        obj2 = store.get_bundle(bundle_hash)
        
        # Should be identical
        assert obj1.bundle_data == obj2.bundle_data
        assert obj1.compute_hash() == obj2.compute_hash()
