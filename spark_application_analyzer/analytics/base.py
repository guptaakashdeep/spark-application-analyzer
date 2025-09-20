# spark_application_analyzer/analytics/strategies/base.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List
from spark_application_analyzer.models.executor_metrics import ExecutorMetrics


class BaseMemoryStrategy(ABC):
    """Abstract base class for all memory recommendation strategies."""

    @abstractmethod
    def generate_recommendation(
        self, executor_metrics: List[ExecutorMetrics]
    ) -> Dict[str, Any]:
        """Generates a memory recommendation based on executor metrics."""
        pass


class BaseExecutorStrategy(ABC):
    """Abstract base class for all executor count recommendation strategies."""

    @abstractmethod
    def generate_recommendation(
        self, executor_metrics: List[ExecutorMetrics]
    ) -> Dict[str, Any]:
        """Generates an executor count recommendation based on executor metrics."""
        pass
