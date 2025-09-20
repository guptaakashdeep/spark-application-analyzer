"""Spark History Server client for collecting application and executor metrics."""

import logging
import requests
from typing import List, Optional, Dict, Any
from dataclasses import asdict

from models.executor_metrics import (
    SparkApplication,
    ApplicationAttempt,
    ExecutorMetrics,
    PeakExecutorMetrics,
)
from utils.conversions import convert_dt_to_ms
from .datasource import IDataSource

logger = logging.getLogger(__name__)


class SparkHistoryServerClient(IDataSource):
    """Client for interacting with Spark History Server REST API. This is a pure data collector."""

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
        env_url = (
            f"/applications/{app_id}/{attempt_id}/environment"
            if attempt_id
            else f"/applications/{app_id}/environment"
        )
        return self._get(env_url)

    def get_executors(self, app_id: str, attempt_id: Optional[int]) -> List[dict]:
        executors_url = (
            f"/applications/{app_id}/{attempt_id}/allexecutors"
            if attempt_id
            else f"/applications/{app_id}/allexecutors"
        )
        return self._get(executors_url)

    def get_peak_memory_metrics(
        self, app_id: str, attempt_id: Optional[int]
    ) -> List[Dict[str, Any]]:
        all_executors = self.get_executors(app_id, attempt_id)
        peak_metrics = [
            PeakExecutorMetrics(**executor["peakMemoryMetrics"])
            for executor in all_executors
            if executor["id"] != "driver" and "peakMemoryMetrics" in executor.keys()
        ]
        return [asdict(m) for m in peak_metrics]

    def get_executor_details(
        self, app_id: str, attempt_id: Optional[int]
    ) -> List[Dict[str, Any]]:
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
        application_end_time = application_detail.attempts[0].end_time
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
                    total_cores=executor.get("totalCores"),
                    max_tasks=executor.get("maxTasks"),
                    max_memory=executor.get("maxMemory"),
                    add_time=convert_dt_to_ms(executor.get("addTime")),
                    remove_time=convert_dt_to_ms(
                        executor.get("removeTime", application_end_time)
                    ),
                    peak_executor_metrics=PeakExecutorMetrics(
                        **executor["peakMemoryMetrics"]
                    ),
                )
            )
        return executor_metrics
