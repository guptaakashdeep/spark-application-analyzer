"""Data models for Spark application metrics and executor data.

This module contains dataclasses and data structures for:
- ExecutorMetrics: Individual executor performance metrics
- ApplicationMetrics: Application-level metrics and metadata
- MemoryMetrics: Memory usage and allocation data
- PerformanceMetrics: CPU, I/O, and other performance indicators
"""

import re
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, Type, TypeVar

T = TypeVar("T")


def camelcase(cls: Type[T]) -> Type[T]:
    """
    Decorator to allow a dataclass to be initialized from camelCase keys.
    Must be placed above the @dataclass decorator.
    """

    def _camel_to_snake(name: str) -> str:
        """
        Converts a camelCase string to snake_case, correctly handling acronyms.
        """
        name = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name)
        name = re.sub(r"([A-Z])([A-Z][a-z])", r"\1_\2", name)
        return name.lower()

    original_init = cls.__init__

    def __init__(self, *args, **kwargs: Any):
        if args:
            raise TypeError(
                f"{cls.__name__} only supports keyword arguments for initialization."
            )

        # Convert the incoming camelCase keys to snake_case.
        snake_case_kwargs = {_camel_to_snake(k): v for k, v in kwargs.items()}
        # Call the original dataclass __init__ with the corrected keys.
        original_init(self, **snake_case_kwargs)

    cls.__init__ = __init__
    return cls


@dataclass
class SparkApplication:
    """Represents a Spark application."""

    id: str
    name: str
    status: str
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    user: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


@camelcase
@dataclass
class PeakExecutorMetrics:
    """Represents peak executor metrics for an executor. All memory stats are in bytes."""

    jvm_heap_memory: int
    jvm_off_heap_memory: int
    on_heap_execution_memory: int
    off_heap_execution_memory: int
    on_heap_storage_memory: int
    off_heap_storage_memory: int
    on_heap_unified_memory: int
    off_heap_unified_memory: int
    direct_pool_memory: int
    mapped_pool_memory: int
    process_tree_jvmv_memory: int
    process_tree_jvmrss_memory: int
    process_tree_python_v_memory: int
    process_tree_python_rss_memory: int
    process_tree_other_v_memory: int
    process_tree_other_rss_memory: int
    minor_gc_count: int
    minor_gc_time: int  # in ms
    major_gc_count: int
    major_gc_time: int  # in ms
    total_gc_time: int  # in ms

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


@dataclass
class ExecutorMetrics:
    """Represents executor metrics from Spark History Server."""

    executor_id: str
    application_id: str
    host: str
    cores: int
    max_memory: int  # in bytes
    max_heap_memory: int  # in bytes
    max_off_heap_memory: int  # in bytes
    process_tree_jvm_vmemory: int  # in bytes
    process_tree_jvm_rssmemory: int  # in bytes
    on_heap_execution_memory: int  # in bytes
    off_heap_execution_memory: int  # in bytes
    on_heap_storage_memory: int  # in bytes
    off_heap_storage_memory: int  # in bytes
    status: str
    start_time: Optional[int] = None
    end_time: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
