"""Data conversion utilities for the Spark Application Analyzer.

This module provides conversion functions between different data formats:
- Dataclass to PyArrow table conversion
- PyArrow table to dataclass conversion
- JSON serialization/deserialization helpers
- Memory unit conversions (bytes to GB, etc.)
- Data validation and type checking utilities
"""

from datetime import datetime


def convert_dt_to_ms(ts):
    """Convert datetime string to milliseconds since epoch.
    Assume format '2023-10-03T22:34:40.240GMT"""
    return int(
        datetime.strptime(ts.replace("GMT", ""), "%Y-%m-%dT%H:%M:%S.%f").timestamp()
        * 1000
    )
