"""Arrow/Parquet storage layer for metrics and recommendations.

This module handles data persistence and I/O operations:
- Conversion between dataclasses and PyArrow tables
- Parquet file generation for metrics storage
- Feather format support for fast I/O
- Data serialization/deserialization utilities
- Storage directory management and file organization
"""
