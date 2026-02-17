"""
Test garbage collection safety.

Verifies that GC never deletes reachable objects.
"""

import pytest
import tempfile

from snapshot_store import SnapshotStoreEngine


class TestGarbageCollectionSafety:
    """Test that GC is safe and never deletes reachable objects."""
    
    @pytest.fixture
    def store(self):
        """Create a temporary store for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = SnapshotStoreEngine(tmpdir)
            engine.initialize()
            yield engine
    
    def test_gc_preserves_referenced_objects(self, store):
        """GC never deletes objects referenced by named snapshots."""
        # Create bundles and snapshot
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        bundle2 = store.put_bundle({'sequence': 2, 'operations': []})
        snapshot = store.put_snapshot([bundle1, bundle2])
        
        # Create named reference (GC root)
        store.create_snapshot_ref('main', snapshot)
        
        # Run GC
        result = store.garbage_collect(dry_run=False)
        
        # All objects should still exist
        assert store.has_object(bundle1)
        assert store.has_object(bundle2)
        assert store.has_object(snapshot)
        
        # Nothing should have been deleted
        assert len(result['deleted']) == 0
    
    def test_gc_deletes_unreachable_objects(self, store):
        """GC deletes objects not referenced by any named snapshot."""
        # Create some objects with reference
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        snapshot1 = store.put_snapshot([bundle1])
        store.create_snapshot_ref('main', snapshot1)
        
        # Create orphaned objects (no reference)
        bundle2 = store.put_bundle({'sequence': 2, 'operations': []})
        snapshot2 = store.put_snapshot([bundle2])
        # Don't create named reference for snapshot2
        
        # Run GC
        result = store.garbage_collect(dry_run=False)
        
        # Referenced objects should exist
        assert store.has_object(bundle1)
        assert store.has_object(snapshot1)
        
        # Unreachable objects should be deleted
        assert not store.has_object(bundle2)
        assert not store.has_object(snapshot2)
        
        # Should have deleted 2 objects
        assert len(result['deleted']) == 2
    
    def test_gc_dry_run(self, store):
        """Dry run reports what would be deleted without deleting."""
        # Create orphaned object
        bundle = store.put_bundle({'sequence': 1, 'operations': []})
        
        # Run dry run
        result = store.garbage_collect(dry_run=True)
        
        # Object should still exist
        assert store.has_object(bundle)
        
        # Should report as unreachable but not deleted
        assert bundle in result['unreachable']
        assert len(result['deleted']) == 0
    
    def test_gc_preserves_parent_chain(self, store):
        """GC preserves entire parent chain."""
        # Create chain
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        snap1 = store.put_snapshot([bundle1])
        
        bundle2 = store.put_bundle({'sequence': 2, 'operations': []})
        snap2 = store.put_snapshot([bundle2], parent=snap1)
        
        bundle3 = store.put_bundle({'sequence': 3, 'operations': []})
        snap3 = store.put_snapshot([bundle3], parent=snap2)
        
        # Only reference the final snapshot
        store.create_snapshot_ref('main', snap3)
        
        # Run GC
        result = store.garbage_collect(dry_run=False)
        
        # Entire chain should be preserved
        assert store.has_object(bundle1)
        assert store.has_object(snap1)
        assert store.has_object(bundle2)
        assert store.has_object(snap2)
        assert store.has_object(bundle3)
        assert store.has_object(snap3)
        
        # Nothing should be deleted
        assert len(result['deleted']) == 0
    
    def test_gc_multiple_roots(self, store):
        """GC preserves objects reachable from any root."""
        # Create two independent snapshot trees
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        snap1 = store.put_snapshot([bundle1])
        store.create_snapshot_ref('branch1', snap1)
        
        bundle2 = store.put_bundle({'sequence': 2, 'operations': []})
        snap2 = store.put_snapshot([bundle2])
        store.create_snapshot_ref('branch2', snap2)
        
        # Create orphaned object
        bundle3 = store.put_bundle({'sequence': 3, 'operations': []})
        
        # Run GC
        result = store.garbage_collect(dry_run=False)
        
        # Both roots should be preserved
        assert store.has_object(bundle1)
        assert store.has_object(snap1)
        assert store.has_object(bundle2)
        assert store.has_object(snap2)
        
        # Orphan should be deleted
        assert not store.has_object(bundle3)
        assert len(result['deleted']) == 1
    
    def test_gc_shared_bundles(self, store):
        """GC preserves bundles shared by multiple snapshots."""
        # Create shared bundle
        shared_bundle = store.put_bundle({'sequence': 1, 'operations': []})
        
        # Create two snapshots referencing the same bundle
        snap1 = store.put_snapshot([shared_bundle])
        snap2 = store.put_snapshot([shared_bundle])
        
        # Reference both snapshots
        store.create_snapshot_ref('snap1', snap1)
        store.create_snapshot_ref('snap2', snap2)
        
        # Run GC
        result = store.garbage_collect(dry_run=False)
        
        # Shared bundle should be preserved
        assert store.has_object(shared_bundle)
        assert len(result['deleted']) == 0
    
    def test_gc_after_ref_deletion(self, store):
        """Objects become unreachable after ref deletion."""
        # Create snapshot with reference
        bundle = store.put_bundle({'sequence': 1, 'operations': []})
        snapshot = store.put_snapshot([bundle])
        store.create_snapshot_ref('temp', snapshot)
        
        # Objects should be reachable
        result1 = store.garbage_collect(dry_run=True)
        assert len(result1['unreachable']) == 0
        
        # Delete reference
        store.delete_snapshot_ref('temp')
        
        # Objects should now be unreachable
        result2 = store.garbage_collect(dry_run=True)
        assert bundle in result2['unreachable']
        assert snapshot in result2['unreachable']
    
    def test_gc_safety_verification(self, store):
        """GC safety verification catches problems."""
        # Create valid root
        bundle = store.put_bundle({'sequence': 1, 'operations': []})
        snapshot = store.put_snapshot([bundle])
        store.create_snapshot_ref('main', snapshot)
        
        # Verify GC safety
        issues = store.verify_gc_safety()
        assert len(issues) == 0
    
    def test_gc_mark_phase(self, store):
        """GC mark phase correctly identifies reachable objects."""
        # Create object graph
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        bundle2 = store.put_bundle({'sequence': 2, 'operations': []})
        snapshot = store.put_snapshot([bundle1, bundle2])
        store.create_snapshot_ref('main', snapshot)
        
        # Create orphan
        orphan = store.put_bundle({'sequence': 99, 'operations': []})
        
        # Run GC dry run to see marking
        result = store.garbage_collect(dry_run=True)
        
        # Check reachable set
        assert snapshot in result['reachable']
        assert bundle1 in result['reachable']
        assert bundle2 in result['reachable']
        
        # Check unreachable set
        assert orphan in result['unreachable']
    
    def test_gc_preserves_deep_graph(self, store):
        """GC preserves deep object graphs."""
        # Create a complex graph
        bundles = []
        for i in range(20):
            bundle = store.put_bundle({'sequence': i, 'operations': []})
            bundles.append(bundle)
        
        # Create snapshot referencing all bundles
        snapshot = store.put_snapshot(bundles)
        store.create_snapshot_ref('main', snapshot)
        
        # Run GC
        result = store.garbage_collect(dry_run=False)
        
        # All objects should be preserved
        for bundle in bundles:
            assert store.has_object(bundle)
        assert store.has_object(snapshot)
        
        assert len(result['deleted']) == 0
    
    def test_gc_idempotent(self, store):
        """Running GC multiple times is idempotent."""
        # Create orphan
        orphan = store.put_bundle({'sequence': 1, 'operations': []})
        
        # First GC should delete it
        result1 = store.garbage_collect(dry_run=False)
        assert len(result1['deleted']) == 1
        assert orphan in result1['deleted']
        
        # Second GC should find nothing to delete
        result2 = store.garbage_collect(dry_run=False)
        assert len(result2['deleted']) == 0
    
    def test_gc_never_deletes_during_dry_run(self, store):
        """Dry run never modifies storage."""
        # Create orphans
        orphan1 = store.put_bundle({'sequence': 1, 'operations': []})
        orphan2 = store.put_bundle({'sequence': 2, 'operations': []})
        
        # Multiple dry runs
        for _ in range(5):
            result = store.garbage_collect(dry_run=True)
            assert len(result['deleted']) == 0
            assert orphan1 in result['unreachable']
            assert orphan2 in result['unreachable']
        
        # Objects should still exist
        assert store.has_object(orphan1)
        assert store.has_object(orphan2)


class TestGarbageCollectionEdgeCases:
    """Test GC edge cases and corner conditions."""
    
    @pytest.fixture
    def store(self):
        """Create a temporary store for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = SnapshotStoreEngine(tmpdir)
            engine.initialize()
            yield engine
    
    def test_gc_empty_store(self, store):
        """GC on empty store."""
        result = store.garbage_collect(dry_run=False)
        
        assert len(result['reachable']) == 0
        assert len(result['unreachable']) == 0
        assert len(result['deleted']) == 0
    
    def test_gc_no_roots(self, store):
        """GC with no roots deletes everything."""
        # Create objects without references
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        bundle2 = store.put_bundle({'sequence': 2, 'operations': []})
        
        # Run GC
        result = store.garbage_collect(dry_run=False)
        
        # Everything should be deleted
        assert not store.has_object(bundle1)
        assert not store.has_object(bundle2)
        assert len(result['deleted']) == 2
    
    def test_gc_all_reachable(self, store):
        """GC when all objects are reachable."""
        # Create objects and reference them
        bundle = store.put_bundle({'sequence': 1, 'operations': []})
        snapshot = store.put_snapshot([bundle])
        store.create_snapshot_ref('main', snapshot)
        
        # Run GC
        result = store.garbage_collect(dry_run=False)
        
        # Nothing should be deleted
        assert len(result['deleted']) == 0
        assert len(result['unreachable']) == 0
    
    def test_gc_mixed_reachable_unreachable(self, store):
        """GC with mix of reachable and unreachable objects."""
        # Reachable branch
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        snap1 = store.put_snapshot([bundle1])
        store.create_snapshot_ref('main', snap1)
        
        # Unreachable branch
        bundle2 = store.put_bundle({'sequence': 2, 'operations': []})
        snap2 = store.put_snapshot([bundle2])
        
        # Run GC
        result = store.garbage_collect(dry_run=False)
        
        # Reachable should exist
        assert store.has_object(bundle1)
        assert store.has_object(snap1)
        
        # Unreachable should be gone
        assert not store.has_object(bundle2)
        assert not store.has_object(snap2)
        
        assert len(result['deleted']) == 2
