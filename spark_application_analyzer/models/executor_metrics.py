"""Data models for Spark application metrics and executor data.

This module contains dataclasses and data structures for:
- ExecutorMetrics: Individual executor performance metrics
- ApplicationMetrics: Application-level metrics and metadata
- MemoryMetrics: Memory usage and allocation data
- PerformanceMetrics: CPU, I/O, and other performance indicators
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any


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
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "user": self.user,
        }


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
        return {
            "executor_id": self.executor_id,
            "application_id": self.application_id,
            "host": self.host,
            "cores": self.cores,
            "max_memory": self.max_memory,
            "max_heap_memory": self.max_heap_memory,
            "max_off_heap_memory": self.max_off_heap_memory,
            "process_tree_jvm_vmemory": self.process_tree_jvm_vmemory,
            "process_tree_jvm_rssmemory": self.process_tree_jvm_rssmemory,
            "on_heap_execution_memory": self.on_heap_execution_memory,
            "off_heap_execution_memory": self.off_heap_execution_memory,
            "on_heap_storage_memory": self.on_heap_storage_memory,
            "off_heap_storage_memory": self.off_heap_storage_memory,
            "status": self.status,
            "start_time": self.start_time,
            "end_time": self.end_time,
        }
