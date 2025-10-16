"""Data conversion utilities for the Spark Application Analyzer.

This module provides conversion functions between different data formats:
- Dataclass to PyArrow table conversion
- PyArrow table to dataclass conversion
- JSON serialization/deserialization helpers
- Memory unit conversions (bytes to GB, etc.)
- Data validation and type checking utilities
"""
import re
from datetime import datetime
from typing import Any
from typing import Type
from typing import TypeVar

T = TypeVar("T")


def camelcase(cls: Type[T]) -> Type[T]:
    """
    Decorator to allow a dataclass to be initialized from camelCase keys.
    Must be placed above the @dataclass decorator.
    """

    def _camel_to_snake(name: str) -> str:
        """
        Converts a camelCase string to snake_case, correctly handling acronyms.
        """
        name = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name)
        name = re.sub(r"([A-Z])([A-Z][a-z])", r"\1_\2", name)
        return name.lower()

    original_init = cls.__init__

    def __init__(self, *args, **kwargs: Any):
        if args:
            raise TypeError(
                f"{cls.__name__} only supports keyword arguments for initialization."
            )

        # Convert the incoming camelCase keys to snake_case.
        snake_case_kwargs = {_camel_to_snake(k): v for k, v in kwargs.items()}
        # Call the original dataclass __init__ with the corrected keys.
        original_init(self, **snake_case_kwargs)

    cls.__init__ = __init__
    return cls


def convert_dt_to_ms(ts):
    """Convert datetime string to milliseconds since epoch.
    Assume format '2023-10-03T22:34:40.240GMT"""
    return int(
        datetime.strptime(ts.replace("GMT", ""), "%Y-%m-%dT%H:%M:%S.%f").timestamp()
        * 1000
    )
