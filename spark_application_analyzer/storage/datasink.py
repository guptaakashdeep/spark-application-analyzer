from abc import ABC, abstractmethod
from typing import Any


class IDataSink(ABC):
    """
    Interface for data sinks that store recommendation results.
    """

    @abstractmethod
    def save(self, data: Any) -> None:
        """Save the given data to the sink."""
        pass
