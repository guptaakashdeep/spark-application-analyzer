from dataclasses import dataclass, asdict
from datetime import date
from typing import Dict, Any, Optional


@dataclass
class Recommendation:
    """
    Data class to hold the full recommendation details, aligned with analytics output.
    """

    # Input parameters & App Details
    application_id: str
    app_name: str
    metrics_collection_dt: date
    emr_id: Optional[str]

    # Memory recommendations from recommend_executor_mem
    suggested_heap_in_bytes: float
    suggested_overhead_in_bytes: float
    suggested_heap_in_gb: int
    suggested_overhead_in_gb: int
    buffer: float

    # Executor number recommendations from recommend_num_executors
    current_p95_maxExecutors: int
    avg_idle_pct: float
    p95_idle_pct: float
    recommended_maxExecutors: int
    target_idle_pct: float

    # Additional details from environment
    additional_details: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
