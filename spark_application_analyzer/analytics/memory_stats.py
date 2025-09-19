"""Memory analysis engine for Spark executor right-sizing.

This module implements the LinkedIn-inspired right-sizing algorithms:
- Percentile calculations (p50, p90, p95) for memory metrics
- Buffer strategy implementation (25-30% for <8 runs, 10-15% for ≥8 runs)
- Right-sizing formulas for heap, total, and overhead memory
- Gap analysis between percentiles for skew detection
- T-shirt sizing recommendations based on cluster profiles
"""

from typing import List


def recommend_executor_mem(jvm_heap_peaks: List[int], jvm_rss_peaks: List[int]):
    # TODO: call percentile check here
    max_heap_memory = max(jvm_heap_peaks)
    max_total_memory = max(jvm_rss_peaks)
    max_overhead_memory = max_total_memory - max_heap_memory
    # TODO: Check the number of runs for the applications - has to have some unique_id for a job!!
    num_runs = 0
    # TODO: must be configurable?
    buffer = 0.25 if num_runs < 8 else 0.15
    suggested_heap = max_heap_memory + buffer * (max_heap_memory / max_total_memory)
    suggested_overhead = max_overhead_memory + buffer * (
        max_overhead_memory / max_total_memory
    )
    # TODO: implement GuardRails
    # - max increment/decrement of memory size
    # - Checking compared to "Last Known Good (LKG) value"
    # - executor size is not exceeding a node memory limit
    # TODO: Convert this to a dataclass that can be returned
    return {
        "suggested_heap_in_bytes": suggested_heap,
        "suggested_overhead_in_bytes": suggested_overhead,
        "suggested_heap_in_gb": round(suggested_heap/(1024**3)),
        "suggested_overhead_in_gb": round(suggested_overhead/(1024**3))
        "buffer": buffer,
    }
