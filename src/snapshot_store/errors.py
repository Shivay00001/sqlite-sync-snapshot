"""
Error types for snapshot store operations.

All errors are explicit and never silent.
"""


class SnapshotStoreError(Exception):
    """Base exception for all snapshot store errors."""
    pass


class ObjectNotFoundError(SnapshotStoreError):
    """Raised when a requested object does not exist."""
    
    def __init__(self, object_hash: str):
        self.object_hash = object_hash
        super().__init__(f"Object not found: {object_hash}")


class ObjectCorruptedError(SnapshotStoreError):
    """Raised when an object's content does not match its hash."""
    
    def __init__(self, object_hash: str, expected: str, actual: str):
        self.object_hash = object_hash
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Object corrupted: {object_hash}\n"
            f"Expected hash: {expected}\n"
            f"Actual hash: {actual}"
        )


class InvalidObjectError(SnapshotStoreError):
    """Raised when an object is malformed or invalid."""
    
    def __init__(self, reason: str, object_hash: str = None):
        self.reason = reason
        self.object_hash = object_hash
        msg = f"Invalid object: {reason}"
        if object_hash:
            msg += f" (hash: {object_hash})"
        super().__init__(msg)


class SnapshotVerificationError(SnapshotStoreError):
    """Raised when snapshot verification fails."""
    
    def __init__(self, snapshot_hash: str, reason: str):
        self.snapshot_hash = snapshot_hash
        self.reason = reason
        super().__init__(f"Snapshot verification failed: {snapshot_hash}\nReason: {reason}")


class ReferenceMissingError(SnapshotStoreError):
    """Raised when a referenced object is missing."""
    
    def __init__(self, referencing_hash: str, missing_hash: str):
        self.referencing_hash = referencing_hash
        self.missing_hash = missing_hash
        super().__init__(
            f"Object {referencing_hash} references missing object {missing_hash}"
        )


class TamperDetectedError(SnapshotStoreError):
    """Raised when tampering is detected in the object store."""
    
    def __init__(self, details: str):
        self.details = details
        super().__init__(f"Tampering detected: {details}")


class GarbageCollectionError(SnapshotStoreError):
    """Raised when garbage collection encounters an error."""
    
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"Garbage collection error: {reason}")


class InvariantViolationError(SnapshotStoreError):
    """Raised when a system invariant is violated."""
    
    def __init__(self, invariant: str, details: str):
        self.invariant = invariant
        self.details = details
        super().__init__(f"Invariant violation: {invariant}\nDetails: {details}")


class StorageError(SnapshotStoreError):
    """Raised when filesystem operations fail."""
    
    def __init__(self, operation: str, path: str, cause: Exception = None):
        self.operation = operation
        self.path = path
        self.cause = cause
        msg = f"Storage error during {operation}: {path}"
        if cause:
            msg += f"\nCause: {cause}"
        super().__init__(msg)


class InvalidReferenceError(SnapshotStoreError):
    """Raised when an object reference is invalid."""
    
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(f"Invalid reference: {reason}")
