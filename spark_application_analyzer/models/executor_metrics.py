"""Data models for Spark application metrics and executor data.

This module contains dataclasses and data structures for:
- ExecutorMetrics: Individual executor performance metrics
- ApplicationMetrics: Application-level metrics and metadata
- MemoryMetrics: Memory usage and allocation data
- PerformanceMetrics: CPU, I/O, and other performance indicators
"""

from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from spark_application_analyzer.utils.conversions import camelcase


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

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ApplicationAttempt":
        """Create ApplicationAttempt from dictionary with camelCase keys."""
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


@dataclass
class SparkApplication:
    """Represents a Spark application."""

    id: str
    name: str
    attempts: List[ApplicationAttempt]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SparkApplication":
        """Create SparkApplication from dictionary with nested conversion."""
        # Convert attempts list from dicts to ApplicationAttempt objects
        attempts = [
            ApplicationAttempt.from_dict(attempt)
            if isinstance(attempt, dict)
            else attempt
            for attempt in data.get("attempts", [])
        ]

        return cls(id=data["id"], name=data["name"], attempts=attempts)

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
    total_gc_time: int
    total_cores: int
    max_tasks: int
    max_memory: int  # in bytes
    add_time: int
    remove_time: int
    peak_executor_metrics: PeakExecutorMetrics

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
