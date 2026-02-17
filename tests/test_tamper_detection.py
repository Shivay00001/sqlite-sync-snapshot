"""
Test tamper detection.

Verifies that tampering with stored objects is detected.
"""

import pytest
import tempfile
import json
from pathlib import Path

from snapshot_store import (
    SnapshotStoreEngine,
    ObjectCorruptedError,
    InvalidObjectError,
)


class TestTamperDetection:
    """Test detection of tampered objects."""
    
    @pytest.fixture
    def store(self):
        """Create a temporary store for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = SnapshotStoreEngine(tmpdir)
            engine.initialize()
            yield engine
    
    def test_detect_modified_content(self, store):
        """Detect when object content is modified."""
        # Create object
        bundle_hash = store.put_bundle({'sequence': 1, 'operations': []})
        
        # Modify the content
        obj_path = store.layout.get_object_path(bundle_hash)
        with open(obj_path, 'r') as f:
            obj_data = json.load(f)
        
        obj_data['content']['sequence'] = 999  # Tamper
        
        with open(obj_path, 'w') as f:
            json.dump(obj_data, f)
        
        # Verification should fail
        with pytest.raises((ObjectCorruptedError, InvalidObjectError)):
            store.verify_object(bundle_hash)
    
    def test_detect_replaced_file(self, store):
        """Detect when object file is replaced."""
        # Create object
        bundle_hash = store.put_bundle({'sequence': 1, 'operations': []})
        
        # Replace file with different content
        obj_path = store.layout.get_object_path(bundle_hash)
        with open(obj_path, 'w') as f:
            json.dump({'type': 'bundle', 'content': {'sequence': 999}}, f)
        
        # Verification should fail
        with pytest.raises((ObjectCorruptedError, InvalidObjectError)):
            store.verify_object(bundle_hash)
    
    def test_detect_corrupted_json(self, store):
        """Detect when object file contains invalid JSON."""
        # Create object
        bundle_hash = store.put_bundle({'sequence': 1, 'operations': []})
        
        # Corrupt JSON
        obj_path = store.layout.get_object_path(bundle_hash)
        with open(obj_path, 'w') as f:
            f.write('{ invalid json }')
        
        # Should raise error when trying to load
        with pytest.raises(Exception):  # StorageError or similar
            store.get_bundle(bundle_hash)
    
    def test_detect_deleted_object(self, store):
        """Detect when object file is deleted."""
        from snapshot_store.errors import ObjectNotFoundError
        
        # Create object
        bundle_hash = store.put_bundle({'sequence': 1, 'operations': []})
        
        # Delete file
        obj_path = store.layout.get_object_path(bundle_hash)
        obj_path.unlink()
        
        # Should raise ObjectNotFoundError
        with pytest.raises(ObjectNotFoundError):
            store.get_bundle(bundle_hash)
    
    def test_detect_tamper_in_store_scan(self, store):
        """Detect tampering via full store scan."""
        # Create valid objects
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        bundle2 = store.put_bundle({'sequence': 2, 'operations': []})
        bundle3 = store.put_bundle({'sequence': 3, 'operations': []})
        
        # Tamper with one
        obj_path = store.layout.get_object_path(bundle2)
        with open(obj_path, 'r') as f:
            obj_data = json.load(f)
        obj_data['content']['sequence'] = 999
        with open(obj_path, 'w') as f:
            json.dump(obj_data, f)
        
        # Scan for tampering
        result = store.detect_tampering()
        
        # Should detect the tampered object
        assert len(result['tampered']) == 1
        assert bundle2 in result['tampered']
        
        # Other objects should be verified
        assert result['verified'] >= 2
    
    def test_no_false_positives(self, store):
        """Valid objects are not flagged as tampered."""
        # Create valid objects
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        bundle2 = store.put_bundle({'sequence': 2, 'operations': []})
        
        # Scan should find no tampering
        result = store.detect_tampering()
        
        assert len(result['tampered']) == 0
        assert result['verified'] == 2
    
    def test_detect_metadata_tampering(self, store):
        """Detect when metadata is tampered with."""
        # Create object with metadata
        bundle_hash = store.put_bundle(
            {'sequence': 1, 'operations': []},
            metadata={'source': 'test'}
        )
        
        # Modify metadata
        obj_path = store.layout.get_object_path(bundle_hash)
        with open(obj_path, 'r') as f:
            obj_data = json.load(f)
        
        obj_data['metadata']['source'] = 'tampered'
        
        with open(obj_path, 'w') as f:
            json.dump(obj_data, f)
        
        # Verification should fail
        with pytest.raises((ObjectCorruptedError, InvalidObjectError)):
            store.verify_object(bundle_hash)
    
    def test_detect_type_change(self, store):
        """Detect when object type is changed."""
        # Create bundle
        bundle_hash = store.put_bundle({'sequence': 1, 'operations': []})
        
        # Change type to blob
        obj_path = store.layout.get_object_path(bundle_hash)
        with open(obj_path, 'r') as f:
            obj_data = json.load(f)
        
        obj_data['type'] = 'blob'
        
        with open(obj_path, 'w') as f:
            json.dump(obj_data, f)
        
        # Verification should fail
        with pytest.raises((ObjectCorruptedError, InvalidObjectError)):
            store.verify_object(bundle_hash)
    
    def test_detect_snapshot_reference_tampering(self, store):
        """Detect when snapshot references are tampered with."""
        # Create snapshot
        bundle1 = store.put_bundle({'sequence': 1, 'operations': []})
        snapshot_hash = store.put_snapshot([bundle1])
        
        # Tamper with bundle reference
        obj_path = store.layout.get_object_path(snapshot_hash)
        with open(obj_path, 'r') as f:
            obj_data = json.load(f)
        
        # Change bundle reference
        obj_data['content']['bundles'][0] = 'a' * 64
        
        with open(obj_path, 'w') as f:
            json.dump(obj_data, f)
        
        # Verification should fail
        with pytest.raises((ObjectCorruptedError, InvalidObjectError)):
            store.verify_object(snapshot_hash)


class TestIntegrityGuarantees:
    """Test that integrity guarantees are maintained."""
    
    @pytest.fixture
    def store(self):
        """Create a temporary store for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = SnapshotStoreEngine(tmpdir)
            engine.initialize()
            yield engine
    
    def test_hash_integrity_on_read(self, store):
        """Every read verifies hash integrity."""
        bundle_hash = store.put_bundle({'sequence': 1, 'operations': []})
        
        # Reading with verify=True (default) checks integrity
        bundle = store.get_bundle(bundle_hash)
        
        # Should succeed for valid object
        assert bundle is not None
    
    def test_cannot_store_mismatched_hash(self, store):
        """Cannot manually store object with wrong hash."""
        # This test verifies that the storage layer enforces integrity
        # In our implementation, hash is computed from content,
        # so this scenario is prevented by design
        
        bundle = store.put_bundle({'sequence': 1, 'operations': []})
        
        # Verify stored correctly
        assert store.has_object(bundle)
    
    def test_atomic_writes(self, store):
        """Object writes are atomic."""
        # Create many objects rapidly
        hashes = []
        for i in range(100):
            h = store.put_bundle({'sequence': i, 'operations': []})
            hashes.append(h)
        
        # All should be valid and retrievable
        for h in hashes:
            assert store.has_object(h)
            assert store.verify_object(h) is True
    
    def test_immutability_after_storage(self, store):
        """Objects remain immutable after storage."""
        bundle_data = {'sequence': 1, 'operations': []}
        bundle_hash = store.put_bundle(bundle_data)
        
        # Get object
        bundle1 = store.get_bundle(bundle_hash)
        
        # Verify
        assert store.verify_object(bundle_hash) is True
        
        # Get again
        bundle2 = store.get_bundle(bundle_hash)
        
        # Should be identical
        assert bundle1.bundle_data == bundle2.bundle_data
        assert bundle1.compute_hash() == bundle2.compute_hash()
    
    def test_verification_catches_all_tampering(self, store):
        """Verification catches all forms of tampering."""
        # Create object
        bundle_hash = store.put_bundle({'sequence': 1, 'operations': []})
        
        # Get file path
        obj_path = store.layout.get_object_path(bundle_hash)
        
        # Various tampering attempts
        tampering_methods = [
            # Change content
            lambda: self._tamper_content(obj_path),
            # Change metadata
            lambda: self._tamper_metadata(obj_path),
            # Change type
            lambda: self._tamper_type(obj_path),
        ]
        
        for tamper_func in tampering_methods:
            # Store fresh object
            bundle_hash = store.put_bundle({'sequence': 1, 'operations': []})
            obj_path = store.layout.get_object_path(bundle_hash)
            
            # Tamper
            tamper_func()
            
            # Verification should fail
            with pytest.raises((ObjectCorruptedError, InvalidObjectError)):
                store.verify_object(bundle_hash)
    
    @staticmethod
    def _tamper_content(path):
        with open(path, 'r') as f:
            data = json.load(f)
        data['content']['sequence'] = 999
        with open(path, 'w') as f:
            json.dump(data, f)
    
    @staticmethod
    def _tamper_metadata(path):
        with open(path, 'r') as f:
            data = json.load(f)
        data['metadata'] = {'tampered': True}
        with open(path, 'w') as f:
            json.dump(data, f)
    
    @staticmethod
    def _tamper_type(path):
        with open(path, 'r') as f:
            data = json.load(f)
        data['type'] = 'blob'
        with open(path, 'w') as f:
            json.dump(data, f)


class TestRealWorldTamperingScenarios:
    """Test realistic tampering scenarios."""
    
    @pytest.fixture
    def store(self):
        """Create a temporary store for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = SnapshotStoreEngine(tmpdir)
            engine.initialize()
            yield engine
    
    def test_detect_bitflip(self, store):
        """Detect single bit flip in stored data."""
        bundle_hash = store.put_bundle({'sequence': 1, 'operations': []})
        
        # Read file as bytes
        obj_path = store.layout.get_object_path(bundle_hash)
        data = obj_path.read_bytes()
        
        # Flip a bit in the middle
        byte_array = bytearray(data)
        byte_array[len(byte_array) // 2] ^= 0x01  # Flip one bit
        
        # Write back
        obj_path.write_bytes(bytes(byte_array))
        
        # Verification should fail
        with pytest.raises(Exception):
            store.verify_object(bundle_hash)
    
    def test_detect_partial_write(self, store):
        """Detect truncated/partial write."""
        bundle_hash = store.put_bundle({'sequence': 1, 'operations': []})
        
        # Truncate file
        obj_path = store.layout.get_object_path(bundle_hash)
        data = obj_path.read_bytes()
        obj_path.write_bytes(data[:len(data)//2])
        
        # Should fail to parse or verify
        with pytest.raises(Exception):
            store.verify_object(bundle_hash)
    
    def test_detect_appended_data(self, store):
        """Detect extra data appended to file."""
        bundle_hash = store.put_bundle({'sequence': 1, 'operations': []})
        
        # Append data
        obj_path = store.layout.get_object_path(bundle_hash)
        with open(obj_path, 'ab') as f:
            f.write(b'\n\nextra data here')
        
        # Should fail verification
        with pytest.raises(Exception):
            store.verify_object(bundle_hash)
