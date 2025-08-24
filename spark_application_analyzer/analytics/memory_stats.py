"""Memory analysis engine for Spark executor right-sizing.

This module implements the LinkedIn-inspired right-sizing algorithms:
- Percentile calculations (p50, p90, p95) for memory metrics
- Buffer strategy implementation (25-30% for <8 runs, 10-15% for ≥8 runs)
- Right-sizing formulas for heap, total, and overhead memory
- Gap analysis between percentiles for skew detection
- T-shirt sizing recommendations based on cluster profiles
"""
