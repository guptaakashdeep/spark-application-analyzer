from dataclasses import dataclass
from dataclasses import field
from typing import Optional

from spark_application_analyzer.utils.conversions import camelcase


@camelcase
@dataclass
class StageMetrics:
    stage_id: int
    status: str
    memory_bytes_spilled: int
    disk_bytes_spilled: int
    name: str
    num_tasks: int
    num_failed_tasks: int
    num_killed_tasks: int
    first_task_launched_time: str
    completion_time: Optional[str] = field(default=None)
