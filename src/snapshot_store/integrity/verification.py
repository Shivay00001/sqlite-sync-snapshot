"""
Integrity verification for objects and snapshots.

Provides tamper detection and recursive verification.
"""

from typing import Set, List, Tuple

from ..errors import (
    ObjectCorruptedError,
    InvalidObjectError,
    ReferenceMissingError,
    TamperDetectedError,
)
from .hashing import compute_hash, compute_object_hash
from .canonical import canonical_json


def verify_object_integrity(obj_data: dict, expected_hash: str) -> None:
    """
    Verify that an object's content matches its hash.
    
    Raises ObjectCorruptedError if mismatch detected.
    """
    actual_hash = compute_object_hash(obj_data)
    if actual_hash != expected_hash:
        raise ObjectCorruptedError(expected_hash, expected_hash, actual_hash)


def verify_object_structure(obj_data: dict) -> None:
    """
    Verify that an object has valid structure.
    
    All objects must have:
    - 'type' field
    - 'content' field
    - optional 'metadata' field
    
    Raises InvalidObjectError if structure is invalid.
    """
    if not isinstance(obj_data, dict):
        raise InvalidObjectError("Object must be a dictionary")
    
    if 'type' not in obj_data:
        raise InvalidObjectError("Object missing 'type' field")
    
    if 'content' not in obj_data:
        raise InvalidObjectError("Object missing 'content' field")
    
    valid_types = {'blob', 'bundle', 'snapshot', 'tree'}
    if obj_data['type'] not in valid_types:
        raise InvalidObjectError(f"Invalid object type: {obj_data['type']}")
    
    if 'metadata' in obj_data and not isinstance(obj_data['metadata'], dict):
        raise InvalidObjectError("Metadata must be a dictionary")


def extract_references(obj_data: dict) -> Set[str]:
    """
    Extract all object references from an object.
    
    References are found in:
    - snapshot: 'bundles' and 'parent' fields
    - tree: 'children' field
    - bundle: no references (leaf object)
    - blob: no references (leaf object)
    
    Returns set of referenced hashes.
    """
    refs = set()
    obj_type = obj_data.get('type')
    content = obj_data.get('content', {})
    
    if obj_type == 'snapshot':
        # Snapshots reference bundles and optionally a parent
        if isinstance(content, dict):
            if 'bundles' in content:
                refs.update(content['bundles'])
            if 'parent' in content and content['parent']:
                refs.add(content['parent'])
    
    elif obj_type == 'tree':
        # Trees reference child objects
        if isinstance(content, dict) and 'children' in content:
            refs.update(content['children'])
    
    # blob and bundle are leaf objects with no references
    
    return refs


def verify_references_exist(obj_hash: str, references: Set[str], exists_func) -> None:
    """
    Verify that all referenced objects exist.
    
    exists_func should be a callable that takes a hash and returns bool.
    
    Raises ReferenceMissingError if any reference is missing.
    """
    for ref_hash in references:
        if not exists_func(ref_hash):
            raise ReferenceMissingError(obj_hash, ref_hash)


def verify_snapshot_recursive(
    snapshot_hash: str,
    load_func,
    exists_func,
    visited: Set[str] = None
) -> Tuple[bool, List[str]]:
    """
    Recursively verify a snapshot and all its references.
    
    load_func: callable that loads object data by hash
    exists_func: callable that checks if object exists by hash
    visited: set of already-visited hashes (for cycle detection)
    
    Returns (is_valid, errors) where errors is list of error messages.
    """
    if visited is None:
        visited = set()
    
    errors = []
    
    # Detect cycles
    if snapshot_hash in visited:
        errors.append(f"Cycle detected: {snapshot_hash}")
        return False, errors
    
    visited.add(snapshot_hash)
    
    # Load and verify the snapshot object
    try:
        obj_data = load_func(snapshot_hash)
    except Exception as e:
        errors.append(f"Failed to load {snapshot_hash}: {e}")
        return False, errors
    
    # Verify object structure
    try:
        verify_object_structure(obj_data)
    except InvalidObjectError as e:
        errors.append(f"Invalid structure in {snapshot_hash}: {e}")
        return False, errors
    
    # Verify object integrity (hash matches content)
    try:
        verify_object_integrity(obj_data, snapshot_hash)
    except ObjectCorruptedError as e:
        errors.append(f"Corruption in {snapshot_hash}: {e}")
        return False, errors
    
    # Verify this is actually a snapshot
    if obj_data.get('type') != 'snapshot':
        errors.append(f"Object {snapshot_hash} is not a snapshot")
        return False, errors
    
    # Extract and verify all references
    refs = extract_references(obj_data)
    
    try:
        verify_references_exist(snapshot_hash, refs, exists_func)
    except ReferenceMissingError as e:
        errors.append(str(e))
        return False, errors
    
    # Recursively verify referenced objects
    for ref_hash in refs:
        try:
            ref_obj = load_func(ref_hash)
            verify_object_structure(ref_obj)
            verify_object_integrity(ref_obj, ref_hash)
            
            # If reference is also a snapshot, verify it recursively
            if ref_obj.get('type') == 'snapshot':
                is_valid, sub_errors = verify_snapshot_recursive(
                    ref_hash, load_func, exists_func, visited
                )
                if not is_valid:
                    errors.extend(sub_errors)
        
        except Exception as e:
            errors.append(f"Failed to verify reference {ref_hash}: {e}")
            return False, errors
    
    is_valid = len(errors) == 0
    return is_valid, errors


def detect_tampering(obj_hash: str, stored_data: bytes, metadata: dict = None) -> bool:
    """
    Detect if an object has been tampered with.
    
    Compares stored hash against recomputed hash of data.
    
    Returns True if tampering detected, False otherwise.
    """
    # For stored objects, we recompute the hash from the stored data
    actual_hash = compute_hash(stored_data)
    return actual_hash != obj_hash
