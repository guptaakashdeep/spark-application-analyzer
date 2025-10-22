from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Dict
from typing import List


class IDataSource(ABC):
    """
    Interface for data sources that provide Spark application metrics.
    """

    @abstractmethod
    def list_applications(self) -> List[Dict[str, Any]]:
        """List all available applications."""
        pass

    @abstractmethod
    def get_application(self, app_id: str) -> Dict[str, Any]:
        """Get detailed information for a specific application."""
        pass

    @abstractmethod
    def get_environment(self, app_id: str, attempt_id: str = None) -> Dict[str, Any]:
        """Get the environment details for a specific application."""
        pass

    @abstractmethod
    def get_peak_memory_metrics(
        self, app_id: str, attempt_id: str = None
    ) -> List[Dict[str, Any]]:
        """Get peak memory usage metrics for each executor."""
        pass

    @abstractmethod
    def get_executor_details(
        self, app_id: str, attempt_id: str = None
    ) -> List[Dict[str, Any]]:
        """Get detailed information and lifecycle stats for each executor."""
        pass

    @abstractmethod
    def get_stage_metrics(
        self, app_id: str, attempt_id: str = None
    ) -> List[Dict[str, Any]]:
        """Get detailed stage information for a specific application."""
        pass

    @abstractmethod
    def get_job_metrics(
        self, app_id: str, attempt_id: str = None
    ) -> List[Dict[str, Any]]:
        """Get detailed job information for a specific application."""
        pass

    @abstractmethod
    def list_completed_applications(self) -> List[Dict[str, Any]]:
        """Get all applications with completed status."""
        pass
