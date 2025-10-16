# spark_application_analyzer/analytics/strategies/base.py
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Dict
from typing import List

from spark_application_analyzer.models.executor_metrics import ExecutorMetrics
from spark_application_analyzer.models.job_metrics import JobMetrics
from spark_application_analyzer.models.stage_metrics import StageMetrics


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


class BasePerformanceStrategy(ABC):
    """Abstract base class for all executor count recommendation strategies."""

    @abstractmethod
    def identify_bottlenecks(
        self,
        stage_metrics: List[StageMetrics],
        job_metrics: List[JobMetrics],
        executor_metrics: List[ExecutorMetrics],
    ) -> Dict[str, Any]:
        """Generates an executor count recommendation based on executor metrics."""
        pass
