"""Spark History Server client for collecting application and executor metrics."""

import requests
import logging
from typing import List, Optional
from spark_application_analyzer.models.executor_metrics import (
    SparkApplication,
    PeakExecutorMetrics,
)
from spark_application_analyzer.analytics.memory_stats import recommend_executor_mem

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

    def get_environment(self, app_id) -> dict:
        # TODO: maybe replace 1 with attempt
        return self._get(f"/applications/{app_id}/1/environment")

    def get_executors(self, app_id: str) -> List[dict]:
        return self._get(f"/applications/{app_id}/allexecutors")

    def get_peak_executor_metrics(self, app_id: str) -> List[PeakExecutorMetrics]:
        all_executors = self.get_executors(app_id)
        return [
            PeakExecutorMetrics(**executor["peakMemoryMetrics"])
            for executor in all_executors
            if executor["id"] != "driver"
        ]

    def get_recommended_executor_size(self, app_id: str) -> dict:
        peak_executor_metrics = self.get_peak_executor_metrics(app_id)
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
