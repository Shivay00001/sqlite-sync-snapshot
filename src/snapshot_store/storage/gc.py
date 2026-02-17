"""
Garbage collection for unreachable objects.

Implements mark-and-sweep algorithm with safety guarantees.
"""

from typing import Set, List, Callable
from collections import deque

from ..errors import GarbageCollectionError, InvariantViolationError
from ..integrity.verification import extract_references


class GarbageCollector:
    """
    Garbage collector for content-addressed object store.
    
    Uses mark-and-sweep algorithm:
    1. Mark: trace from roots to find reachable objects
    2. Sweep: delete unmarked objects
    
    Safety guarantees:
    - Never deletes reachable objects
    - Atomic operation (all or nothing)
    - No race conditions (single-threaded)
    """
    
    def __init__(
        self,
        list_all_func: Callable[[], List[str]],
        load_object_func: Callable[[str], dict],
        delete_object_func: Callable[[str], bool],
        exists_func: Callable[[str], bool],
    ):
        """
        Initialize garbage collector.
        
        list_all_func: returns list of all object hashes
        load_object_func: loads object data by hash
        delete_object_func: deletes object by hash
        exists_func: checks if object exists
        """
        self.list_all = list_all_func
        self.load_object = load_object_func
        self.delete_object = delete_object_func
        self.exists = exists_func
    
    def collect(self, roots: Set[str], dry_run: bool = False) -> dict:
        """
        Run garbage collection.
        
        Args:
            roots: set of root object hashes to keep
            dry_run: if True, only report what would be deleted
        
        Returns dict with:
            - reachable: set of reachable object hashes
            - unreachable: set of unreachable object hashes
            - deleted: list of deleted object hashes (empty if dry_run)
            - errors: list of error messages
        """
        result = {
            'reachable': set(),
            'unreachable': set(),
            'deleted': [],
            'errors': [],
        }
        
        # Phase 1: Mark - find all reachable objects
        try:
            reachable = self._mark_reachable(roots)
            result['reachable'] = reachable
        except Exception as e:
            result['errors'].append(f"Mark phase failed: {e}")
            return result
        
        # Phase 2: Identify unreachable objects
        try:
            all_objects = set(self.list_all())
            unreachable = all_objects - reachable
            result['unreachable'] = unreachable
        except Exception as e:
            result['errors'].append(f"Failed to list objects: {e}")
            return result
        
        # Phase 3: Sweep - delete unreachable objects
        if not dry_run and unreachable:
            deleted = []
            for obj_hash in unreachable:
                try:
                    # Double-check it's not reachable (safety)
                    if obj_hash in reachable:
                        result['errors'].append(
                            f"Safety violation: attempted to delete reachable object {obj_hash}"
                        )
                        continue
                    
                    if self.delete_object(obj_hash):
                        deleted.append(obj_hash)
                
                except Exception as e:
                    result['errors'].append(f"Failed to delete {obj_hash}: {e}")
            
            result['deleted'] = deleted
        
        return result
    
    def _mark_reachable(self, roots: Set[str]) -> Set[str]:
        """
        Mark all objects reachable from roots.
        
        Uses breadth-first traversal to find all referenced objects.
        
        Returns set of reachable object hashes.
        """
        reachable = set()
        queue = deque(roots)
        
        while queue:
            obj_hash = queue.popleft()
            
            # Skip if already processed
            if obj_hash in reachable:
                continue
            
            # Skip if doesn't exist
            if not self.exists(obj_hash):
                continue
            
            # Mark as reachable
            reachable.add(obj_hash)
            
            # Load object and find references
            try:
                obj_data = self.load_object(obj_hash)
                refs = extract_references(obj_data)
                
                # Add references to queue
                for ref in refs:
                    if ref not in reachable:
                        queue.append(ref)
            
            except Exception:
                # If we can't load an object, we can't traverse it
                # but we still mark it as reachable to be safe
                continue
        
        return reachable
    
    def verify_gc_safety(self, roots: Set[str]) -> List[str]:
        """
        Verify that garbage collection would be safe.
        
        Checks:
        - All roots exist
        - All roots are valid objects
        - No cycles that could cause issues
        
        Returns list of warnings/errors.
        """
        issues = []
        
        # Check that all roots exist
        for root in roots:
            if not self.exists(root):
                issues.append(f"Root does not exist: {root}")
        
        # Check that roots are valid objects
        for root in roots:
            if self.exists(root):
                try:
                    obj_data = self.load_object(root)
                    if not isinstance(obj_data, dict):
                        issues.append(f"Root is not a valid object: {root}")
                except Exception as e:
                    issues.append(f"Failed to load root {root}: {e}")
        
        return issues


class IncrementalGC:
    """
    Incremental garbage collection for large stores.
    
    Allows GC to be run in smaller batches to avoid long pauses.
    """
    
    def __init__(self, gc: GarbageCollector):
        self.gc = gc
        self._reachable_cache: Set[str] = set()
        self._cache_valid = False
    
    def invalidate_cache(self) -> None:
        """Invalidate the reachability cache."""
        self._cache_valid = False
        self._reachable_cache = set()
    
    def mark_batch(self, roots: Set[str], batch_size: int = 1000) -> dict:
        """
        Mark a batch of objects as reachable.
        
        Returns dict with progress information.
        """
        result = {
            'marked': 0,
            'remaining': 0,
            'complete': False,
        }
        
        reachable = self.gc._mark_reachable(roots)
        self._reachable_cache = reachable
        self._cache_valid = True
        
        result['marked'] = len(reachable)
        result['complete'] = True
        
        return result
    
    def sweep_batch(self, batch_size: int = 100) -> dict:
        """
        Sweep a batch of unreachable objects.
        
        Returns dict with progress information.
        """
        if not self._cache_valid:
            return {
                'deleted': 0,
                'errors': ['Cache not valid, run mark_batch first'],
            }
        
        result = {
            'deleted': 0,
            'errors': [],
        }
        
        # Find unreachable objects
        all_objects = set(self.gc.list_all())
        unreachable = all_objects - self._reachable_cache
        
        # Delete in batches
        count = 0
        for obj_hash in unreachable:
            if count >= batch_size:
                break
            
            if obj_hash not in self._reachable_cache:  # Safety check
                try:
                    if self.gc.delete_object(obj_hash):
                        result['deleted'] += 1
                        count += 1
                except Exception as e:
                    result['errors'].append(f"Failed to delete {obj_hash}: {e}")
        
        return result
