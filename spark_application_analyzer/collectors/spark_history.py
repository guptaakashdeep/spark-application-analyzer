"""Spark History Server client for collecting application and executor metrics."""

import requests
import logging
from typing import List, Optional
from spark_application_analyzer.models.executor_metrics import (
    SparkApplication,
    ExecutorMetrics,
)

logger = logging.getLogger(__name__)


class SparkHistoryServerClient:
    """Client for interacting with Spark History Server REST API."""

    def __init__(self, base_url: str, timeout: int = 10):
        """base_url like http://<host>:18080."""
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str) -> dict:
        url = f"{self.base_url}/api/v1{path}"
        resp = requests.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def list_applications(self) -> List[dict]:
        return self._get("/applications")

    def get_executors(self, app_id: str) -> List[dict]:
        return self._get(f"/applications/{app_id}/allexecutors")

    def get_executor_metrics(self, app_id: str) -> List[ExecutorMetrics]:
        return []
