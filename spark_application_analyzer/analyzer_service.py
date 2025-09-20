from datetime import datetime
from typing import Optional

from collectors.datasource import IDataSource
from models.recommendation import Recommendation
from analytics.memory_stats import recommend_executor_mem
from analytics.required_executors import recommend_num_executors
from models.executor_metrics import ExecutorMetrics, PeakExecutorMetrics
from utils.cli_colors import Colors


class AnalyzerService:
    """Orchestrates the analysis of a Spark application to provide recommendations."""

    def __init__(self, datasource: IDataSource):
        self.datasource = datasource

    def generate_recommendations(
        self, app_id: str, attempt_id: Optional[str], emr_id: Optional[str]
    ) -> Recommendation:
        """
        Fetches data, runs analysis, and returns a recommendation object.
        """
        # 1. Get all raw data from the datasource
        app_details = self.datasource.get_application(app_id)
        app_env = self.datasource.get_environment(app_id, attempt_id)

        # 2. Perform validation (moved from CLI)
        env_parameters = {
            "spark.executor.instances",
            "spark.executor.memory",
            "spark.dynamicAllocation.enabled",
            "spark.dynamicAllocation.minExecutors",
            "spark.dynamicAllocation.maxExecutors",
            "spark.executor.cores",
            "spark.executor.memoryOverhead",
            "spark.executor.memoryOverheadFactor",
            "spark.executor.processTreeMetrics.enabled",
        }
        defined_exec_params = {
            env[0]: env[1]
            for env in list(
                filter(
                    lambda prop: prop[0] in env_parameters, app_env["sparkProperties"]
                )
            )
        }
        can_recommend = (
            defined_exec_params.get(
                "spark.executor.processTreeMetrics.enabled", "false"
            ).lower()
            == "true"
        )
        if not can_recommend:
            raise ValueError(
                f"{Colors.RED}{Colors.BOLD}Recommendations cannot be determined. Spark Application must run with configuration spark.executor.processTreeMetrics.enabled=true {Colors.END}"
            )

        # 3. Get metrics and run analysis
        # Memory analysis
        peak_executor_metrics_raw = self.datasource.get_peak_memory_metrics(
            app_id, attempt_id
        )
        jvm_heap_peaks = [
            p["jvm_heap_memory"] + p["jvm_off_heap_memory"]
            for p in peak_executor_metrics_raw
        ]
        jvm_rss_peaks = [
            p["process_tree_jvmrss_memory"] for p in peak_executor_metrics_raw
        ]
        mem_recommendations = recommend_executor_mem(jvm_heap_peaks, jvm_rss_peaks)

        # Executor number analysis
        executor_details = self.datasource.get_executor_details(app_id, attempt_id)
        exec_num_recommendations = recommend_num_executors(executor_details)

        # 4. Assemble the final Recommendation object
        recommendation = Recommendation(
            application_id=app_id,
            app_name=app_details["name"],
            metrics_collection_dt=datetime.now().date(),
            emr_id=emr_id,
            additional_details=defined_exec_params,
            **mem_recommendations,
            **exec_num_recommendations,
        )

        return recommendation
