from typing import Optional
from spark_application_analyzer.collectors.spark_history import SparkHistoryServerClient
from spark_application_analyzer.analyzer_service import AnalyzerService
from spark_application_analyzer.analytics.memory import MaxMemoryStrategy
from spark_application_analyzer.analytics.executors import IdleTimeStrategy
from spark_application_analyzer.storage.arrow_io import ParquetSink
from spark_application_analyzer.models.recommendation import Recommendation
from spark_application_analyzer.utils.aws import get_history_server_url


def analyze_application(
    application_id: str,
    emr_id: Optional[str] = None,
    base_url: Optional[str] = None,
    sink_path: Optional[str] = None,
) -> Recommendation:
    """
    A high-level function to analyze a Spark application and generate recommendations.

    This function simplifies programmatic access by handling the initialization
    of all necessary components.

    :param application_id: The ID of the Spark application to analyze.
    :param emr_id: Optional EMR cluster ID. If provided, the Spark History Server URL
                   will be discovered automatically.
    :param base_url: The base URL of the Spark History Server. Used if emr_id is not provided.
                     Defaults to http://localhost:18080 if neither is provided.
    :param sink_path: Optional path to save the recommendation parquet file.
    :return: A Recommendation object containing the analysis results.
    """
    if emr_id:
        final_base_url = get_history_server_url(emr_id)
    elif base_url:
        final_base_url = base_url
    else:
        final_base_url = "http://localhost:18080"

    # 1. Initialize the data source
    source = SparkHistoryServerClient(final_base_url)

    # 2. Instantiate the default strategies
    memory_strategy = MaxMemoryStrategy()
    executor_strategy = IdleTimeStrategy()

    # 3. Initialize the Analyzer Service
    analyzer = AnalyzerService(source, memory_strategy, executor_strategy)

    # 4. Get application details to find the latest attempt
    app_details = analyzer.datasource.get_application(application_id)
    attempt_id = app_details["attempts"][0].get("attemptId")

    # 5. Generate recommendations
    recommendation = analyzer.generate_recommendations(
        app_id=application_id, attempt_id=attempt_id, emr_id=emr_id
    )

    # 6. Optionally save the results
    if sink_path:
        sink = ParquetSink(sink_path)
        sink.save(recommendation)

    return recommendation
