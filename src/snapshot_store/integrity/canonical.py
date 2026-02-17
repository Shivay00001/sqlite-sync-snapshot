"""
Canonical encoding for deterministic hashing.

Ensures same input always produces same hash.
"""

import json
from typing import Any


def canonical_json(obj: Any) -> bytes:
    """
    Encode an object to canonical JSON bytes.
    
    Rules:
    - Keys sorted alphabetically
    - No whitespace
    - UTF-8 encoding
    - Consistent float representation
    - No trailing newlines
    
    Same input always produces same output.
    """
    json_str = json.dumps(
        obj,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=False,
        allow_nan=False,
    )
    return json_str.encode('utf-8')


def canonical_json_str(obj: Any) -> str:
    """
    Encode an object to canonical JSON string.
    
    Useful for debugging and logging.
    """
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=False,
        allow_nan=False,
    )


def validate_canonical_structure(obj: Any) -> None:
    """
    Validate that an object can be canonically encoded.
    
    Raises ValueError if the object contains non-serializable types.
    """
    try:
        canonical_json(obj)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Object cannot be canonically encoded: {e}")
