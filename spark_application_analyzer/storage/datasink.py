from abc import ABC
from abc import abstractmethod
from typing import Any


class IDataSink(ABC):
    """
    Interface for data sinks that store recommendation results.
    """

    @abstractmethod
    def save(self, data: Any) -> None:
        """Save the given data to the sink."""
        pass

    @abstractmethod
    def log(self, log_data: Any) -> None:
        """Log the application process status"""
        pass
