from datetime import datetime
from typing import Optional

from spark_application_analyzer.analytics.base import BaseExecutorStrategy
from spark_application_analyzer.analytics.base import BaseMemoryStrategy
from spark_application_analyzer.analytics.base import BasePerformanceStrategy
from spark_application_analyzer.collectors.datasource import IDataSource
from spark_application_analyzer.models.executor_metrics import SparkApplication
from spark_application_analyzer.models.recommendation import AnalysisResults
from spark_application_analyzer.models.recommendation import Bottlenecks
from spark_application_analyzer.models.recommendation import Recommendation
from spark_application_analyzer.utils.cli_colors import Colors


class AnalyzerService:
    """Orchestrates the analysis of a Spark application to provide recommendations."""

    def __init__(
        self,
        datasource: IDataSource,
        memory_strategy: BaseMemoryStrategy,
        executor_strategy: BaseExecutorStrategy,
        performance_strategy: BasePerformanceStrategy,
    ):
        self.datasource = datasource
        self.memory_strategy = memory_strategy
        self.executor_strategy = executor_strategy
        self.performance_strategy = performance_strategy

    def generate_recommendations(
        self,
        app_details: SparkApplication,
        attempt_id: Optional[str],
        emr_id: Optional[str],
    ) -> AnalysisResults:
        """
        Fetches data, runs analysis, and returns a analysis results object containing both recommendation and bottlenecks.
        """
        # 1. Get application environment data
        # app_details = self.datasource.get_application(app_id)
        app_id = app_details.id
        # TODO: attempts object should be created from app_details filterd with attempt_id
        # this attemptId object should be used for getting further details.
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
            app_name=app_details.name,
            time_taken_mins=round((app_details.attempts[0]["duration"] / 1000) / 60, 2),
            metrics_collection_dt=datetime.now().date(),
            emr_id=emr_id,
            current_configuration=defined_exec_params,
            **mem_recommendations,
            **exec_num_recommendations,
        )

        # Bottleneck identification
        job_details = self.datasource.get_job_metrics(app_id, attempt_id)
        num_jobs = len(job_details)
        stage_details = self.datasource.get_stage_metrics(app_id, attempt_id)
        num_stages = len(stage_details)
        bottlenecks = Bottlenecks(
            **self.performance_strategy.identify_bottlenecks(
                stage_details, job_details, executor_details
            ),
            **{"num_jobs": num_jobs, "num_stages": num_stages},
        )

        analysis_results = AnalysisResults(
            recommendation=recommendation, bottlenecks=bottlenecks
        )

        return analysis_results
