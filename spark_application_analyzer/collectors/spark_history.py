"""Spark History Server client for collecting application and executor metrics."""

import requests
import logging
from datetime import datetime
from typing import List, Optional
from models.executor_metrics import (
    SparkApplication,
    ApplicationAttempt,
    ExecutorMetrics,
    PeakExecutorMetrics,
)
from analytics.memory_stats import recommend_executor_mem
from analytics.required_executors import recommend_num_executors
from utils.conversions import convert_dt_to_ms

logger = logging.getLogger(__name__)


class SparkHistoryServerClient:
    """Client for interacting with Spark History Server REST API."""

    def __init__(self, base_url: str, timeout: int = 10):
        """base_url like http://<host>:18080."""
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str) -> dict:
        url = f"{self.base_url}/api/v1{path}"
        resp = requests.get(url, verify=False, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def list_applications(self) -> List[dict]:
        return self._get("/applications")

    def get_application(self, app_id: str) -> dict:
        return self._get(f"/applications/{app_id}")

    def get_environment(self, app_id: str, attempt_id: Optional[int]) -> dict:
        env_url = f"/applications/{app_id}/{attempt_id}/environment" if attempt_id else f"/applications/{app_id}/environment"
        return self._get(env_url)

    def get_executors(self, app_id: str, attempt_id: Optional[int]) -> List[dict]:
        executors_url = f"/applications/{app_id}/{attempt_id}/allexecutors" if attempt_id else f"/applications/{app_id}/allexecutors"
        return self._get(executors_url)

    def get_peak_executor_metrics(self, app_id: str, attempt_id: Optional[int]) -> List[PeakExecutorMetrics]:
        all_executors = self.get_executors(app_id, attempt_id)
        return [
                PeakExecutorMetrics(**executor["peakMemoryMetrics"])
                for executor in all_executors
                if executor["id"] != "driver"
            ]

    def get_recommended_executor_size(self, app_id: str, attempt_id: Optional[int]) -> dict:
        peak_executor_metrics = self.get_peak_executor_metrics(app_id, attempt_id)
        jvm_heap_peaks = [
            peak_metric.jvm_heap_memory + peak_metric.jvm_off_heap_memory
            for peak_metric in peak_executor_metrics
        ]
        jvm_rss_peaks = [
            peak_metric.process_tree_jvmrss_memory
            for peak_metric in peak_executor_metrics
        ]
        recommendation = recommend_executor_mem(jvm_heap_peaks, jvm_rss_peaks)
        return recommendation

    def get_recommended_executor_numbers(self, app_id: str, attempt_id: Optional[int]) -> dict:
        executor_metrics = []
        # Get application details
        
        spark_application = self.get_application(app_id) #SparkApplication(**)
        application_detail = SparkApplication(
            id=spark_application["id"],
            name=spark_application["name"],
            attempts=[ApplicationAttempt(**attempt) for attempt in spark_application["attempts"]]
        )
        
        application_end_time = application_detail.attempts[0].end_time
        # Get Executor details
        executor_details = self.get_executors(app_id, attempt_id)
        for executor in executor_details:
            executor_metrics.append(
                ExecutorMetrics(
                    id=executor.get("id"),
                    host=executor.get("host"),
                    is_active=executor.get("isActive"),
                    total_duration=executor.get("totalDuration"),
                    total_cores=executor.get("totalCores"),
                    max_tasks=executor.get("maxTasks"),
                    max_memory=executor.get("maxMemory"),
                    add_time=convert_dt_to_ms(executor.get("addTime")),
                    # for executors with isActive=true, removeTime won't be there
                    remove_time=convert_dt_to_ms(executor.get("removeTime", application_end_time)),
                    peak_executor_metrics=PeakExecutorMetrics(
                        **executor["peakMemoryMetrics"])
                )
            )
        return recommend_num_executors(executor_metrics)
