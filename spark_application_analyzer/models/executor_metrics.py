"""Data models for Spark application metrics and executor data.

This module contains dataclasses and data structures for:
- ExecutorMetrics: Individual executor performance metrics
- ApplicationMetrics: Application-level metrics and metadata
- MemoryMetrics: Memory usage and allocation data
- PerformanceMetrics: CPU, I/O, and other performance indicators
"""

import re
from dataclasses import dataclass, asdict, field
from typing import Optional, Dict, Any, Type, TypeVar, List

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


@camelcase
@dataclass
class ApplicationAttempt:
    """Represents an application attempt."""

    start_time: str
    end_time: Optional[str]
    last_updated: str
    duration: int
    spark_user: str
    completed: bool
    app_spark_version: str
    start_time_epoch: int
    last_updated_epoch: int
    end_time_epoch: int
    system_app_start_time: Optional[int] = field(default=None)
    attempt_id: Optional[str] = field(default=None)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


@dataclass
class SparkApplication:
    """Represents a Spark application."""

    id: str
    name: str
    attempts: List[ApplicationAttempt]

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


@camelcase
@dataclass
class ExecutorMetrics:
    """Represents executor metrics from Spark History Server."""

    id: int
    host: str
    is_active: bool
    total_duration: int  # in ms
    total_cores: int
    max_tasks: int
    max_memory: int  # in bytes
    add_time: int
    remove_time: int
    peak_executor_metrics: PeakExecutorMetrics

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
