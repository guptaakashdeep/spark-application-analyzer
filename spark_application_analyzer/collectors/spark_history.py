"""Spark History Server client for collecting application and executor metrics."""
import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import requests
from spark_application_analyzer.models.executor_metrics import ApplicationAttempt
from spark_application_analyzer.models.executor_metrics import ExecutorMetrics
from spark_application_analyzer.models.executor_metrics import PeakExecutorMetrics
from spark_application_analyzer.models.executor_metrics import SparkApplication
from spark_application_analyzer.models.job_metrics import JobMetrics
from spark_application_analyzer.models.stage_metrics import StageMetrics
from spark_application_analyzer.utils.conversions import convert_dt_to_ms

from .datasource import IDataSource

logger = logging.getLogger(__name__)


class SparkHistoryServerClient(IDataSource):
    """Client for interacting with Spark History Server REST API. This is a pure data collector."""

    def __init__(self, base_url: str, timeout: int = 10):
        """base_url like http://<host>:18080."""
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str, params={}) -> dict:
        """Get details from Spark history server from provided url"""
        url = f"{self.base_url}/api/v1{path}"
        try:
            if params:
                resp = requests.get(
                    url, params=params, verify=False, timeout=self.timeout
                )
            else:
                resp = requests.get(url, verify=False, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data from {url}: {e}")
            raise e
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            raise e

    def list_applications(self) -> List[dict]:
        """Get all the applications"""
        return self._get("/applications")

    def get_application(self, app_id: str) -> dict:
        """Get application with id as app_id"""
        return self._get(f"/applications/{app_id}")

    def list_completed_applications(self) -> List[Dict[str, Any]]:
        """Get all the applications with completed status"""
        params = {"status": "completed"}
        return self._get("/applications", params=params)

    def get_environment(self, app_id: str, attempt_id: Optional[int]) -> dict:
        """Get environment details for the provided app_id and attempt_id"""
        env_url = (
            f"/applications/{app_id}/{attempt_id}/environment"
            if attempt_id
            else f"/applications/{app_id}/environment"
        )
        return self._get(env_url)

    def get_executors(self, app_id: str, attempt_id: Optional[int]) -> List[dict]:
        """Get all executors details for the provided app_id and attempt_id"""
        executors_url = (
            f"/applications/{app_id}/{attempt_id}/allexecutors"
            if attempt_id
            else f"/applications/{app_id}/allexecutors"
        )
        return self._get(executors_url)

    def _get_all_stages(self, app_id: str, attempt_id: Optional[int]) -> List[dict]:
        """Get all stages details for the provided app_id and attempt_id"""
        all_stages_url = (
            f"/applications/{app_id}/{attempt_id}/stages"
            if attempt_id
            else f"/applications/{app_id}/stages"
        )
        return self._get(all_stages_url)

    def _get_all_jobs(self, app_id: str, attempt_id: Optional[int]) -> List[dict]:
        """Get all job details for the provided app_id and attempt_id"""
        all_jobs_url = (
            f"/applications/{app_id}/{attempt_id}/jobs"
            if attempt_id
            else f"/applications/{app_id}/jobs"
        )
        return self._get(all_jobs_url)

    def get_peak_memory_metrics(
        self, app_id: str, attempt_id: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Get peakMemory metrics of all executors for the provided app_id and attempt_id"""
        all_executors = self.get_executors(app_id, attempt_id)
        peak_metrics = [
            PeakExecutorMetrics(**executor["peakMemoryMetrics"])
            for executor in all_executors
            if executor["id"] != "driver" and "peakMemoryMetrics" in executor.keys()
        ]
        return peak_metrics

    def get_executor_details(
        self, app_id: str, attempt_id: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Get all executor metrics in an application"""
        executor_metrics = []
        spark_application = self.get_application(app_id)
        application_detail = SparkApplication(
            id=spark_application["id"],
            name=spark_application["name"],
            attempts=[
                ApplicationAttempt(**attempt)
                for attempt in spark_application["attempts"]
            ],
        )
        app_attempt = application_detail.attempts[0]
        # To handle cases where application even after completion is also not moved to
        # Completed applications
        application_end_time = (
            app_attempt.end_time_epoch
            if app_attempt.completed
            else app_attempt.last_updated_epoch
        )
        executor_details = self.get_executors(app_id, attempt_id)
        for executor in executor_details:
            if executor["id"] == "driver" or "peakMemoryMetrics" not in executor.keys():
                continue
            executor_metrics.append(
                ExecutorMetrics(
                    id=executor.get("id"),
                    host=executor.get("host"),
                    is_active=executor.get("isActive"),
                    total_duration=executor.get("totalDuration"),
                    total_gc_time=executor.get("totalGCTime"),
                    total_cores=executor.get("totalCores"),
                    max_tasks=executor.get("maxTasks"),
                    max_memory=executor.get("maxMemory"),
                    add_time=convert_dt_to_ms(executor.get("addTime")),
                    remove_time=convert_dt_to_ms(remove_time_str)
                    if (remove_time_str := executor.get("removeTime"))
                    else application_end_time,
                    peak_executor_metrics=PeakExecutorMetrics(
                        **executor["peakMemoryMetrics"]
                    ),
                )
            )
        return executor_metrics

    def get_stage_metrics(
        self, app_id: str, attempt_id: Optional[int]
    ) -> List[StageMetrics]:
        """Get metrics for all the stages in an application"""
        all_stages = self._get_all_stages(app_id, attempt_id)
        stages = [
            StageMetrics(
                stage_id=stage["stageId"],
                status=stage["status"],
                memory_bytes_spilled=stage["memoryBytesSpilled"],
                disk_bytes_spilled=stage["diskBytesSpilled"],
                name=stage["name"],
                num_tasks=stage["numTasks"],
                num_failed_tasks=stage["numFailedTasks"],
                num_killed_tasks=stage["numKilledTasks"],
                first_task_launched_time=stage["firstTaskLaunchedTime"]
                if "firstTaskLaunchedTime" in stage
                else stage["submissionTime"],
                completion_time=stage.get("completionTime"),
            )
            for stage in all_stages
            if stage["status"] not in ["RUNNING", "SKIPPED"]
        ]
        return stages

    def get_job_metrics(
        self, app_id: str, attempt_id: Optional[int]
    ) -> List[JobMetrics]:
        """Get metrics for all the jobs in an application"""
        all_jobs = self._get_all_jobs(app_id, attempt_id)
        jobs = [
            JobMetrics(**job) for job in all_jobs if job["status"] not in ["RUNNING"]
        ]
        return jobs
