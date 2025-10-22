import heapq
from typing import Any
from typing import Dict
from typing import List

from spark_application_analyzer.models.executor_metrics import ExecutorMetrics
from spark_application_analyzer.models.job_metrics import JobMetrics
from spark_application_analyzer.models.stage_metrics import StageMetrics
from spark_application_analyzer.utils.conversions import convert_dt_to_ms

from .base import BasePerformanceStrategy


class PerformanceBottleneckStrategy(BasePerformanceStrategy):
    """
    A memory recommendation strategy based on the maximum observed memory usage
    plus a buffer.
    """

    def _get_high_spill_stages(self, stages: List[StageMetrics]) -> Dict[str, Any]:
        # All the stages with spills > 100MB
        stage_spill_details = [
            stage for stage in stages if stage.memory_bytes_spilled > 100 * 1024 * 1024
        ]
        high_spill_stages = heapq.nlargest(
            len(stage_spill_details),
            stage_spill_details,
            key=lambda x: x.memory_bytes_spilled,
        )
        return [
            {
                "memory_spilled_mb": round(spill.memory_bytes_spilled / (1024**2), 2),
                "disk_spilled_mb": round(spill.disk_bytes_spilled / (1024**2), 2),
                "stage_id": spill.stage_id,
                "name": spill.name,
            }
            for spill in high_spill_stages
        ]

    def _get_slowest_stages(self, stages: List[StageMetrics]) -> Dict[str, Any]:
        """Get N slowest stage details"""

        def get_stages_duration(stage: StageMetrics) -> int:
            """Calculate stage run duration"""
            return (
                (
                    (
                        convert_dt_to_ms(stage.completion_time)
                        - convert_dt_to_ms(stage.first_task_launched_time)
                    )
                    / 1000
                )
                if stage.completion_time
                else 0
            )

        # TODO: should be driven from config
        n_stages = 5
        return [
            {
                "duration_secs": get_stages_duration(stage),
                "name": stage.name,
                "stage_id": stage.stage_id,
                "task_count": stage.num_tasks,
                "failed_tasks": stage.num_failed_tasks,
                "killed_tasks": stage.num_killed_tasks,
            }
            for stage in heapq.nlargest(n_stages, stages, get_stages_duration)
        ]

    def _get_slowest_jobs(self, jobs: List[JobMetrics]) -> Dict[str, Any]:
        """Get N slowest job details"""

        def get_job_runtime(job: JobMetrics):
            """Get the job execution time in seconds"""
            return (
                (
                    convert_dt_to_ms(job.completion_time)
                    - convert_dt_to_ms(job.submission_time)
                )
                / 1000
                if job.completion_time
                else 0
            )

        n_jobs = 5
        return [
            {
                "job_id": job.job_id,
                "name": job.name,
                "duration_secs": get_job_runtime(job),
                "stages_count": len(job.stage_ids),
                "task_count": job.num_tasks,
                "failed_tasks": job.num_failed_tasks,
            }
            for job in heapq.nlargest(n_jobs, jobs, get_job_runtime)
        ]

    def _get_gc_pressure(self, executors: List[ExecutorMetrics]) -> Dict[str, Any]:
        """Get executor with highest GC pressure.
        Any executor over 10% time spent in GC, needs to be looked."""

        def calculate_gc_pressure(executor_metrics: ExecutorMetrics):
            return (
                executor_metrics.total_gc_time / executor_metrics.total_duration
                if executor_metrics.total_duration
                else 0
            ) * 100

        max_gc_pressure_executor = max(
            executors, key=lambda x: calculate_gc_pressure(x)
        )
        return round(calculate_gc_pressure(max_gc_pressure_executor), 2)

    def identify_bottlenecks(
        self,
        stage_metrics: List[StageMetrics],
        job_metrics: List[JobMetrics],
        executor_metrics: List[ExecutorMetrics],
    ) -> Dict[str, Any]:
        """
        Identify performance bottlenecks in the Spark application by analyzing
        stages, jobs, and executor metrics.

        Returns a dictionary containing bottleneck analysis results.
        """
        bottlenecks = {}

        # Failed stages
        failed_stages = len(
            [metric for metric in stage_metrics if metric.status == "FAILED"]
        )
        bottlenecks["num_failed_stages"] = failed_stages
        # Failed Jobs
        failed_jobs = len(
            [metric for metric in job_metrics if metric.status == "FAILED"]
        )
        bottlenecks["num_failed_jobs"] = failed_jobs

        # Identify high memory spill stages
        high_spill_stages = self._get_high_spill_stages(stage_metrics)
        if high_spill_stages:
            bottlenecks["high_spill_stages"] = high_spill_stages

        # Identify slowest stages
        slowest_stages = self._get_slowest_stages(stage_metrics)
        if slowest_stages:
            bottlenecks["slowest_stages"] = slowest_stages

        # Identify slowest jobs
        slowest_jobs = self._get_slowest_jobs(job_metrics)
        if slowest_jobs:
            bottlenecks["slowest_jobs"] = slowest_jobs

        # Analyze executor utilization patterns
        gc_pressure_ratio = self._get_gc_pressure(executor_metrics)
        if gc_pressure_ratio > 10:
            bottlenecks["gc_pressure_ratio"] = gc_pressure_ratio

        return bottlenecks
