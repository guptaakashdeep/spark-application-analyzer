from typing import Any
from typing import Dict
from typing import List

from spark_application_analyzer.models.executor_metrics import PeakExecutorMetrics

from .base import BaseMemoryStrategy


class MaxMemoryStrategy(BaseMemoryStrategy):
    """
    A memory recommendation strategy based on the maximum observed memory usage
    plus a buffer.
    """

    def generate_recommendation(
        self, executor_metrics: List[PeakExecutorMetrics]
    ) -> Dict[str, Any]:
        """
        Recommends executor memory based on peak JVM heap and RSS memory usage.
        """
        if not executor_metrics:
            return {}

        jvm_heap_peaks = [
            em.jvm_heap_memory + em.jvm_off_heap_memory for em in executor_metrics
        ]
        jvm_rss_peaks = [em.process_tree_jvmrss_memory for em in executor_metrics]

        if not jvm_heap_peaks or not jvm_rss_peaks:
            return {}

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
        return {
            "recommended_heap_bytes": suggested_heap,
            "recommended_overhead_bytes": suggested_overhead,
            "recommended_heap_gb": round(suggested_heap / (1024**3)),
            "recommended_overhead_gb": round(suggested_overhead / (1024**3)),
            "buffer": buffer,
        }
