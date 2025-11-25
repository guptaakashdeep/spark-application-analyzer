import json
from typing import Optional

from mcp.server.fastmcp import FastMCP

from spark_application_analyzer.api import analyze_application
from spark_application_analyzer.collectors.spark_history import SparkHistoryServerClient

# Initialize FastMCP server
mcp = FastMCP("spark-analyzer")


def _get_client(base_url: Optional[str] = None) -> SparkHistoryServerClient:
    """Helper to get a SparkHistoryServerClient instance."""
    url = base_url or "http://localhost:18080"
    return SparkHistoryServerClient(url)


@mcp.tool()
def analyze_spark_app(
    application_id: str,
    emr_id: Optional[str] = None,
    base_url: Optional[str] = None,
) -> str:
    """
    Analyze a Spark application and generate recommendations.

    Args:
        application_id: The ID of the Spark application to analyze.
        emr_id: Optional EMR cluster ID. If provided, the Spark History Server URL will be discovered automatically.
        base_url: The base URL of the Spark History Server. Used if emr_id is not provided. Defaults to http://localhost:18080.
    """
    try:
        results = analyze_application(
            application_id=application_id,
            emr_id=emr_id,
            base_url=base_url,
        )

        recommendation = results.recommendation
        bottlenecks = results.bottlenecks

        # Construct a structured response
        response = {
            "application_id": recommendation.application_id,
            "app_name": recommendation.app_name,
            "recommendations": {
                "executor_memory_gb": recommendation.recommended_heap_memory_gb,
                "executor_overhead_memory_gb": recommendation.recommended_overhead_memory_gb,
                "current_executor_memory": recommendation.current_configuration.get(
                    "spark.executor.memory"
                ),
                "current_overhead_memory": recommendation.current_configuration.get(
                    "spark.executor.memoryOverhead"
                )
                or recommendation.current_configuration.get(
                    "spark.executor.memoryOverheadFactor"
                ),
            },
            "bottlenecks": {
                "num_jobs": bottlenecks.num_jobs,
                "num_failed_jobs": bottlenecks.num_failed_jobs,
                "num_stages": bottlenecks.num_stages,
                "num_failed_stages": bottlenecks.num_failed_stages,
                "gc_pressure_ratio": bottlenecks.gc_pressure_ratio,
                "slowest_jobs": [
                    {
                        "job_id": job.job_id,
                        "name": job.name,
                        "duration_secs": job.duration_secs,
                    }
                    for job in bottlenecks.slowest_jobs
                ]
                if bottlenecks.slowest_jobs
                else [],
                "slowest_stages": [
                    {
                        "stage_id": stage.stage_id,
                        "name": stage.name,
                        "duration_secs": stage.duration_secs,
                    }
                    for stage in bottlenecks.slowest_stages
                ]
                if bottlenecks.slowest_stages
                else [],
            },
        }

        return json.dumps(response, indent=2)

    except Exception as e:
        return f"Error analyzing application: {str(e)}"


@mcp.tool()
def list_spark_apps(
    status: Optional[str] = None, limit: int = 20, base_url: Optional[str] = None
) -> str:
    """
    List Spark applications from the history server.

    Args:
        status: Filter by status (e.g., 'completed', 'running').
        limit: Maximum number of applications to return.
        base_url: The base URL of the Spark History Server. Defaults to http://localhost:18080.
    """
    try:
        client = _get_client(base_url)
        if status == "completed":
            apps = client.list_completed_applications()
        else:
            # The base list_applications doesn't support filtering by status in the client method signature
            # but the API might. For now, we fetch all and filter if needed or just return all.
            # The client.list_applications() calls /applications
            apps = client.list_applications()
            if status:
                apps = [
                    app
                    for app in apps
                    if app.get("attempts")
                    and app["attempts"][-1].get("completed")
                    == (status.lower() == "completed")
                ]

        # Sort by startTime descending to get latest first
        apps.sort(
            key=lambda x: x.get("attempts", [{}])[0].get("startTimeEpoch", 0),
            reverse=True,
        )
        return json.dumps(apps[:limit], indent=2)
    except Exception as e:
        return f"Error listing applications: {str(e)}"


@mcp.tool()
def get_spark_app_details(application_id: str, base_url: Optional[str] = None) -> str:
    """
    Get details for a specific Spark application.

    Args:
        application_id: The ID of the application.
        base_url: The base URL of the Spark History Server. Defaults to http://localhost:18080.
    """
    try:
        client = _get_client(base_url)
        app = client.get_application(application_id)
        return json.dumps(app, indent=2)
    except Exception as e:
        return f"Error getting application details: {str(e)}"


@mcp.tool()
def get_spark_environment(
    application_id: str,
    attempt_id: Optional[int] = None,
    base_url: Optional[str] = None,
) -> str:
    """
    Get environment information for a Spark application.

    Args:
        application_id: The ID of the application.
        attempt_id: Optional attempt ID.
        base_url: The base URL of the Spark History Server. Defaults to http://localhost:18080.
    """
    try:
        client = _get_client(base_url)
        env = client.get_environment(application_id, attempt_id)
        return json.dumps(env, indent=2)
    except Exception as e:
        return f"Error getting environment: {str(e)}"


@mcp.tool()
def get_spark_executors(
    application_id: str,
    attempt_id: Optional[int] = None,
    base_url: Optional[str] = None,
) -> str:
    """
    Get executor information for a Spark application.

    Args:
        application_id: The ID of the application.
        attempt_id: Optional attempt ID.
        base_url: The base URL of the Spark History Server. Defaults to http://localhost:18080.
    """
    try:
        client = _get_client(base_url)
        executors = client.get_executors(application_id, attempt_id)
        return json.dumps(executors, indent=2)
    except Exception as e:
        return f"Error getting executors: {str(e)}"


@mcp.tool()
def get_spark_jobs(
    application_id: str,
    attempt_id: Optional[int] = None,
    base_url: Optional[str] = None,
) -> str:
    """
    Get job information for a Spark application.

    Args:
        application_id: The ID of the application.
        attempt_id: Optional attempt ID.
        base_url: The base URL of the Spark History Server. Defaults to http://localhost:18080.
    """
    try:
        client = _get_client(base_url)
        # Accessing protected method as per plan to expose raw data
        jobs = client._get_all_jobs(application_id, attempt_id)
        return json.dumps(jobs, indent=2)
    except Exception as e:
        return f"Error getting jobs: {str(e)}"


@mcp.tool()
def get_spark_stages(
    application_id: str,
    attempt_id: Optional[int] = None,
    base_url: Optional[str] = None,
) -> str:
    """
    Get stage information for a Spark application.

    Args:
        application_id: The ID of the application.
        attempt_id: Optional attempt ID.
        base_url: The base URL of the Spark History Server. Defaults to http://localhost:18080.
    """
    try:
        client = _get_client(base_url)
        # Accessing protected method as per plan to expose raw data
        stages = client._get_all_stages(application_id, attempt_id)
        return json.dumps(stages, indent=2)
    except Exception as e:
        return f"Error getting stages: {str(e)}"


def main():
    mcp.run()


if __name__ == "__main__":
    main()
