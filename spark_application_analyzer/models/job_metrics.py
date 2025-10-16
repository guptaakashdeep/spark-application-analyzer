from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from spark_application_analyzer.utils.conversions import camelcase


@camelcase
@dataclass
class JobMetrics:
    job_id: int
    name: str
    submission_time: str
    stage_ids: List[int]
    job_tags: List[str]
    status: str
    num_tasks: int
    num_active_tasks: int
    num_completed_tasks: int
    num_skipped_tasks: int
    num_failed_tasks: int
    num_killed_tasks: int
    num_completed_indices: int
    num_active_stages: int
    num_completed_stages: int
    num_skipped_stages: int
    num_failed_stages: int
    killed_tasks_summary: Dict[str, Any]
    completion_time: Optional[str] = field(default=None)
