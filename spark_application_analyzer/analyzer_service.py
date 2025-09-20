from datetime import datetime
from typing import Optional
from spark_application_analyzer.collectors.datasource import IDataSource
from spark_application_analyzer.models.recommendation import Recommendation
from spark_application_analyzer.analytics.base import (
    BaseMemoryStrategy,
    BaseExecutorStrategy,
)
from spark_application_analyzer.utils.cli_colors import Colors


class AnalyzerService:
    """Orchestrates the analysis of a Spark application to provide recommendations."""

    def __init__(
        self,
        datasource: IDataSource,
        memory_strategy: BaseMemoryStrategy,
        executor_strategy: BaseExecutorStrategy,
    ):
        self.datasource = datasource
        self.memory_strategy = memory_strategy
        self.executor_strategy = executor_strategy

    def generate_recommendations(
        self, app_id: str, attempt_id: Optional[str], emr_id: Optional[str]
    ) -> Recommendation:
        """
        Fetches data, runs analysis, and returns a recommendation object.
        """
        # 1. Get application environment data
        app_details = self.datasource.get_application(app_id)
        app_env = self.datasource.get_environment(app_id, attempt_id)

        # 2. Perform validation
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

        # 3. Get metrics and run analysis using strategies
        # Note: Assuming a method that returns a complete list of ExecutorMetrics.
        # This might need to be implemented in the datasource implementation.
        peak_executor_metrics = self.datasource.get_peak_memory_metrics(
            app_id, attempt_id
        )
        executor_details = self.datasource.get_executor_details(app_id, attempt_id)

        mem_recommendations = self.memory_strategy.generate_recommendation(
            peak_executor_metrics
        )
        exec_num_recommendations = self.executor_strategy.generate_recommendation(
            executor_details
        )

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
