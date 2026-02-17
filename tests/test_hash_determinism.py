"""
Test hash determinism.

Verifies that same input always produces same hash.
"""

import pytest
import tempfile
from pathlib import Path

from snapshot_store import SnapshotStoreEngine, Blob, Bundle, Snapshot


class TestHashDeterminism:
    """Test that hashing is deterministic."""
    
    @pytest.fixture
    def store(self):
        """Create a temporary store for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = SnapshotStoreEngine(tmpdir)
            engine.initialize()
            yield engine
    
    def test_blob_hash_determinism(self, store):
        """Same blob data produces same hash."""
        data = b"Hello, World!"
        
        # Store twice
        hash1 = store.put_blob(data)
        hash2 = store.put_blob(data)
        
        assert hash1 == hash2
    
    def test_blob_hash_differs_with_different_data(self, store):
        """Different blob data produces different hash."""
        hash1 = store.put_blob(b"data1")
        hash2 = store.put_blob(b"data2")
        
        assert hash1 != hash2
    
    def test_bundle_hash_determinism(self, store):
        """Same bundle produces same hash."""
        bundle_data = {
            'operations': [
                {'type': 'insert', 'table': 'users', 'data': {'id': 1, 'name': 'Alice'}},
            ],
            'sequence': 1,
        }
        
        # Store twice
        hash1 = store.put_bundle(bundle_data)
        hash2 = store.put_bundle(bundle_data)
        
        assert hash1 == hash2
    
    def test_bundle_hash_independent_of_dict_order(self, store):
        """Bundle hash is independent of dict key order."""
        # Python 3.7+ preserves dict order, but our canonical encoding sorts keys
        bundle1 = {'sequence': 1, 'operations': []}
        bundle2 = {'operations': [], 'sequence': 1}
        
        hash1 = store.put_bundle(bundle1)
        hash2 = store.put_bundle(bundle2)
        
        assert hash1 == hash2
    
    def test_snapshot_hash_determinism(self, store):
        """Same snapshot produces same hash."""
        # Create some bundles
        bundle1_hash = store.put_bundle({'sequence': 1, 'operations': []})
        bundle2_hash = store.put_bundle({'sequence': 2, 'operations': []})
        
        # Create snapshot twice
        hash1 = store.put_snapshot([bundle1_hash, bundle2_hash])
        hash2 = store.put_snapshot([bundle1_hash, bundle2_hash])
        
        assert hash1 == hash2
    
    def test_snapshot_hash_differs_with_different_bundles(self, store):
        """Snapshots with different bundles have different hashes."""
        bundle1_hash = store.put_bundle({'sequence': 1, 'operations': []})
        bundle2_hash = store.put_bundle({'sequence': 2, 'operations': []})
        
        snapshot1_hash = store.put_snapshot([bundle1_hash])
        snapshot2_hash = store.put_snapshot([bundle1_hash, bundle2_hash])
        
        assert snapshot1_hash != snapshot2_hash
    
    def test_snapshot_hash_depends_on_bundle_order(self, store):
        """Snapshot hash depends on bundle order."""
        bundle1_hash = store.put_bundle({'sequence': 1, 'operations': []})
        bundle2_hash = store.put_bundle({'sequence': 2, 'operations': []})
        
        snapshot1_hash = store.put_snapshot([bundle1_hash, bundle2_hash])
        snapshot2_hash = store.put_snapshot([bundle2_hash, bundle1_hash])
        
        assert snapshot1_hash != snapshot2_hash
    
    def test_metadata_does_not_affect_hash_without_metadata(self, store):
        """Hash is deterministic even with empty metadata."""
        data = b"test data"
        
        hash1 = store.put_blob(data, metadata={})
        hash2 = store.put_blob(data, metadata={})
        
        assert hash1 == hash2
    
    def test_metadata_affects_hash_when_present(self, store):
        """Different metadata produces different hashes."""
        data = b"test data"
        
        hash1 = store.put_blob(data, metadata={'source': 'test1'})
        hash2 = store.put_blob(data, metadata={'source': 'test2'})
        
        assert hash1 != hash2
    
    def test_model_hash_methods_match_storage(self, store):
        """Model compute_hash methods match storage hashes."""
        # Blob
        blob = Blob(b"test")
        blob_hash_computed = blob.compute_hash()
        blob_hash_stored = store.put_blob(b"test")
        assert blob_hash_computed == blob_hash_stored
        
        # Bundle
        bundle_data = {'sequence': 1, 'operations': []}
        bundle = Bundle(bundle_data)
        bundle_hash_computed = bundle.compute_hash()
        bundle_hash_stored = store.put_bundle(bundle_data)
        assert bundle_hash_computed == bundle_hash_stored
    
    def test_no_timestamp_in_hash(self, store):
        """Hashes are not affected by timestamps (deterministic)."""
        import time
        
        bundle_data = {'sequence': 1, 'operations': []}
        
        hash1 = store.put_bundle(bundle_data)
        time.sleep(0.01)  # Wait a bit
        hash2 = store.put_bundle(bundle_data)
        
        # Hashes should be identical despite time difference
        assert hash1 == hash2
    
    def test_canonical_json_encoding(self):
        """Test that canonical JSON encoding is deterministic."""
        from snapshot_store.integrity.canonical import canonical_json
        
        obj1 = {'b': 2, 'a': 1, 'c': 3}
        obj2 = {'a': 1, 'c': 3, 'b': 2}
        
        bytes1 = canonical_json(obj1)
        bytes2 = canonical_json(obj2)
        
        assert bytes1 == bytes2
        assert bytes1 == b'{"a":1,"b":2,"c":3}'
    
    def test_hash_length_and_format(self, store):
        """Test that hashes have expected length and format."""
        data = b"test"
        hash_str = store.put_blob(data)
        
        # BLAKE3 and SHA-256 both produce 32-byte (256-bit) hashes
        # Hex-encoded = 64 characters
        assert len(hash_str) == 64
        assert all(c in '0123456789abcdef' for c in hash_str)


class TestHashCollisionResistance:
    """Test that different inputs produce different hashes."""
    
    @pytest.fixture
    def store(self):
        """Create a temporary store for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = SnapshotStoreEngine(tmpdir)
            engine.initialize()
            yield engine
    
    def test_many_blobs_unique_hashes(self, store):
        """Generate many blobs and verify all hashes are unique."""
        hashes = set()
        
        for i in range(100):
            data = f"blob_{i}".encode()
            hash_str = store.put_blob(data)
            
            # Should not have seen this hash before
            assert hash_str not in hashes
            hashes.add(hash_str)
        
        # All hashes should be unique
        assert len(hashes) == 100
    
    def test_similar_bundles_different_hashes(self, store):
        """Even similar bundles produce different hashes."""
        bundle1 = {'sequence': 1, 'operations': [{'id': 1}]}
        bundle2 = {'sequence': 1, 'operations': [{'id': 2}]}
        
        hash1 = store.put_bundle(bundle1)
        hash2 = store.put_bundle(bundle2)
        
        assert hash1 != hash2
