# Snapshot Store

Content-addressed snapshot and object storage layer for sqlite-sync-core.

## Overview

This package provides immutable, content-addressed storage with snapshot management designed to strengthen sqlite-sync-core's ecosystem. It is **not** IPFS, not a generic Merkle database, but a deterministic, local-first integrity layer.

## Core Features

- **Content-Addressed Storage**: All objects stored by their BLAKE3 hash
- **Immutable Objects**: Once written, objects never change
- **Snapshot Management**: Deterministic state references with parent linking
- **Integrity Verification**: Tamper detection and recursive verification
- **Garbage Collection**: Safe mark-and-sweep with reachability analysis
- **sqlite-sync-core Integration**: Seamless conversion of sync bundles to snapshots

## Architecture

### Object Types

1. **Blob**: Raw binary data
2. **Bundle**: sqlite-sync-core sync bundles
3. **Snapshot**: References to bundles with optional parent
4. **Tree**: Hierarchical grouping of objects

### Storage Layout

```
store_root/
  objects/
    ab/
      ab12ef34...  # Objects sharded by hash prefix
    cd/
      cd56gh78...
  snapshots/
    main          # Named snapshot references
    feature-x
  refs/
    tag1          # Additional references
```

### Integrity Guarantees

- Hash always matches content (content-addressed)
- Objects are immutable once written
- Referenced objects exist (enforced during verification)
- Deterministic hashing (same input → same hash)
- GC never deletes reachable objects

## Installation

```bash
pip install blake3
cd sqlite_sync_snapshot
pip install -e .
```

## Quick Start

```python
from snapshot_store import SnapshotStoreEngine

# Initialize store
engine = SnapshotStoreEngine('/path/to/store')
engine.initialize()

# Import sync bundles from sqlite-sync-core
sync_bundles = [
    {'sequence': 1, 'operations': [...]},
    {'sequence': 2, 'operations': [...]},
]

bundle_hashes, snapshot_hash = engine.import_sync_bundles(
    sync_bundles,
    snapshot_name='main'
)

# Verify integrity
result = engine.verify_snapshot(snapshot_hash)
print(f"Valid: {result['valid']}")

# Garbage collect unreferenced objects
gc_result = engine.garbage_collect(dry_run=True)
print(f"Unreachable: {len(gc_result['unreachable'])}")
```

## Usage Examples

### Storing Objects

```python
# Store a blob
blob_hash = engine.put_blob(b"raw data")

# Store a bundle
bundle_hash = engine.put_bundle({
    'sequence': 1,
    'operations': [...]
})

# Create a snapshot
snapshot_hash = engine.put_snapshot(
    bundles=[bundle_hash],
    parent=None,
    metadata={'author': 'alice'}
)

# Create named reference
engine.create_snapshot_ref('v1.0', snapshot_hash)
```

### Retrieving Objects

```python
# Get a bundle
bundle = engine.get_bundle(bundle_hash)
print(bundle.bundle_data)

# Get a snapshot
snapshot = engine.get_snapshot(snapshot_hash)
print(f"Bundles: {snapshot.bundles}")
print(f"Parent: {snapshot.parent}")

# Get by named reference
snapshot_hash = engine.get_snapshot_ref('v1.0')
```

### Integrity Verification

```python
# Verify single object
is_valid = engine.verify_object(object_hash)

# Verify snapshot recursively
result = engine.verify_snapshot(snapshot_hash)
if not result['valid']:
    for error in result['errors']:
        print(f"Error: {error}")

# Detect tampering across store
tamper_result = engine.detect_tampering()
for corrupted_hash in tamper_result['tampered']:
    print(f"Tampered: {corrupted_hash}")

# Detect missing objects
missing_result = engine.detect_missing_objects()
for broken_snapshot in missing_result['broken_snapshots']:
    print(f"Broken: {broken_snapshot}")
```

### Garbage Collection

```python
# Dry run (preview)
result = engine.garbage_collect(dry_run=True)
print(f"Would delete {len(result['unreachable'])} objects")

# Verify safety
issues = engine.verify_gc_safety()
if not issues:
    # Actually run GC
    result = engine.garbage_collect(dry_run=False)
    print(f"Deleted {len(result['deleted'])} objects")
```

### Integration with sqlite-sync-core

```python
# Import bundles and create snapshot
bundles = get_sync_bundles_from_sqlite_sync_core()

bundle_hashes, snapshot_hash = engine.import_sync_bundles(
    bundles,
    snapshot_name='main',
    metadata={'timestamp': '2024-01-01'}
)

# Extend existing snapshot
new_bundles = get_more_sync_bundles()

new_bundle_hashes, new_snapshot = engine.extend_snapshot(
    parent_hash=snapshot_hash,
    new_bundles=new_bundles,
    snapshot_name='main'  # Update reference
)

# Export bundles back to sqlite-sync-core format
exported_bundles = engine.export_snapshot_bundles(snapshot_hash)
```

## Testing

Run the complete test suite:

```bash
cd sqlite_sync_snapshot
pytest tests/ -v
```

Run specific test categories:

```bash
pytest tests/test_hash_determinism.py -v
pytest tests/test_snapshot_verification.py -v
pytest tests/test_gc_safety.py -v
pytest tests/test_tamper_detection.py -v
```

## Design Principles

### Determinism

All hashing is deterministic:
- Canonical JSON encoding with sorted keys
- No timestamps in object identity
- Same input always produces same hash

### Safety

Multiple layers of safety:
- Hash verification on every read
- Atomic writes with temp files
- GC mark-and-sweep with safety checks
- No silent failures

### Local-First

- No networking
- No cloud dependencies
- No background daemons
- Pure filesystem storage

### Infrastructure-Grade

- Comprehensive error handling
- No placeholders or TODOs
- Full test coverage
- Clear failure modes

## API Reference

### SnapshotStoreEngine

Main interface for all operations.

**Initialization:**
- `__init__(store_path)`: Create engine
- `initialize()`: Initialize storage structure

**Object Storage:**
- `put_blob(data, metadata)`: Store blob
- `put_bundle(bundle_data, metadata)`: Store bundle
- `put_snapshot(bundles, parent, metadata)`: Create snapshot
- `put_tree(children, metadata)`: Create tree
- `get_blob(hash)`: Retrieve blob
- `get_bundle(hash)`: Retrieve bundle
- `get_snapshot(hash)`: Retrieve snapshot
- `get_tree(hash)`: Retrieve tree
- `has_object(hash)`: Check existence

**Named References:**
- `create_snapshot_ref(name, hash)`: Create reference
- `get_snapshot_ref(name)`: Get reference
- `delete_snapshot_ref(name)`: Delete reference
- `list_snapshot_refs()`: List all references

**Integrity:**
- `verify_object(hash)`: Verify single object
- `verify_snapshot(hash)`: Verify snapshot recursively
- `detect_tampering()`: Scan for tampering
- `detect_missing_objects()`: Find broken references

**Garbage Collection:**
- `garbage_collect(dry_run)`: Run GC
- `verify_gc_safety()`: Check GC safety

**Integration:**
- `import_sync_bundles(bundles, parent, name, metadata)`: Import from sqlite-sync-core
- `extend_snapshot(parent, bundles, name, metadata)`: Extend snapshot
- `export_snapshot_bundles(hash)`: Export to sqlite-sync-core

**Statistics:**
- `get_statistics()`: Get store stats
- `list_all_objects()`: List all hashes

## Error Handling

All operations use explicit exceptions:

- `ObjectNotFoundError`: Object doesn't exist
- `ObjectCorruptedError`: Hash mismatch detected
- `InvalidObjectError`: Malformed object
- `SnapshotVerificationError`: Snapshot invalid
- `ReferenceMissingError`: Referenced object missing
- `TamperDetectedError`: Tampering detected
- `GarbageCollectionError`: GC failed
- `InvariantViolationError`: System invariant violated
- `StorageError`: Filesystem operation failed

## Performance Considerations

### Storage Overhead

- Each object stored as individual JSON file
- Directory sharding reduces filesystem strain (256 subdirectories)
- Atomic writes use temporary files

### Verification Cost

- Full verification walks entire object graph
- Dry-run GC scans all objects
- Consider periodic verification vs. every-read verification

### Optimization Strategies

1. **Batch Operations**: Import many bundles at once
2. **Lazy Verification**: Use `verify=False` when appropriate
3. **Incremental GC**: Use for very large stores
4. **Object Caching**: Implement caching layer if needed

## Contributing

This is infrastructure-grade code. Contributions must:

- Include comprehensive tests
- Have no placeholders or TODOs
- Maintain deterministic behavior
- Preserve all safety guarantees
- Include documentation

## License

[License information here]

## See Also

- sqlite-sync-core: Underlying sync engine
- BLAKE3: Hash function used
- Content-addressed storage: General concept
