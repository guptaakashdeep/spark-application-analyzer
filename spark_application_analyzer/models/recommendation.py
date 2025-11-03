from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from datetime import date
from typing import Any
from typing import Dict
from typing import List
from typing import Optional


@dataclass
class Recommendation:
    """
    Data class to hold the full recommendation details, aligned with analytics output.
    """

    # Input parameters & App Details
    application_id: str
    app_name: str
    time_taken_mins: float
    metrics_collection_dt: date
    emr_id: Optional[str]

    # Memory recommendations from recommend_executor_mem
    max_heap_memory: int
    max_heap_memory_gb: int
    max_total_memory: int
    max_total_memory_gb: int
    max_overhead_memory: int
    max_overhead_memory_gb: int
    recommended_heap_bytes: float
    recommended_overhead_bytes: float
    recommended_heap_memory_gb: int
    recommended_overhead_memory_gb: int
    buffer: float

    # Executor number recommendations from recommend_num_executors
    avg_idle_pct: float
    target_idle_pct: float
    current_p95_maxExecutors: int
    recommended_maxExecutors: int

    # Additional details from environment
    current_configuration: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


@dataclass
class HighSpillStages:
    stage_id: int
    name: str
    memory_spilled_mb: float
    disk_spilled_mb: float


@dataclass
class SlowestStages:
    stage_id: int
    name: str
    duration_secs: float
    task_count: int
    failed_tasks: int
    killed_tasks: int


@dataclass
class SlowestJobs:
    job_id: int
    name: str
    duration_secs: float
    stages_count: int
    task_count: int
    failed_tasks: int


@dataclass
class StageRetries:
    stage_id: int
    num_retries: int


@dataclass
class Bottlenecks:
    num_jobs: int
    num_stages: int
    num_failed_jobs: int
    num_failed_stages: int
    slowest_jobs: Optional[List[SlowestJobs]] = field(default_factory=list)
    slowest_stages: Optional[List[SlowestStages]] = field(default_factory=list)
    high_spill_stages: Optional[List[HighSpillStages]] = field(default_factory=list)
    gc_pressure_ratio: Optional[float] = field(default=0.0)
    stage_retries: Optional[StageRetries] = field(default_factory=dict)


@dataclass
class AnalysisResults:
    recommendation: Recommendation
    bottlenecks: Bottlenecks
