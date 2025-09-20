# cli.py

import argparse
import sys
import boto3
from collectors.spark_history import SparkHistoryServerClient
from storage.arrow_io import ParquetSink
from utils.cli_colors import Colors
from analyzer_service import AnalyzerService


def get_history_server_url(emr_id: str) -> str:
    """Fetch Spark History Server URL from EMR cluster using boto3."""
    client = boto3.client("emr")
    cluster = client.describe_cluster(ClusterId=emr_id)
    apps = cluster["Cluster"]["Applications"]
    history_server_dns = None
    for app in apps:
        if app["Name"].lower() == "spark":
            history_server_dns = (
                f"http://{cluster['Cluster']['MasterPublicDnsName']}:18080"
            )
            break
    if not history_server_dns:
        raise RuntimeError(
            "Could not determine Spark History Server URL for this EMR cluster."
        )
    return history_server_dns


def main():
    parser = argparse.ArgumentParser(description="Spark Executor Right Sizing CLI")
    parser.add_argument(
        "--emr-id", type=str, help="EMR Cluster ID (to fetch history server endpoint)"
    )
    parser.add_argument("--base-url", type=str, help="Spark History Server base URL")
    parser.add_argument(
        "--action",
        type=str,
        default="list-apps",
        choices=["list-apps", "get-recommendation"],
        help="CLI Action",
    )
    parser.add_argument(
        "--app-id", type=str, help="Spark Application ID (for get-recommendation)"
    )
    parser.add_argument(
        "--sink-path",
        type=str,
        help="Path to save recommendation parquet file (e.g., s3://bucket/path)",
    )

    args = parser.parse_args()

    if args.emr_id:
        base_url = get_history_server_url(args.emr_id)
        print(f"Discovered Spark History Server URL via EMR: {base_url}")
    elif args.base_url:
        base_url = args.base_url
    else:
        base_url = "http://localhost:18080"
        print("Assuming local history server: http://localhost:18080")

    # Initialize the data source
    source = SparkHistoryServerClient(base_url)

    # CLI Actions
    if args.action == "list-apps":
        apps = source.list_applications()
        for app in apps:
            print(
                f"App ID: {app['id']}, Name: {app['name']}, Started: {app['attempts'][0]['startTime']}"
            )
    elif args.action == "get-recommendation":
        if not args.app_id:
            print("Error: --app-id required for get-recommendation", file=sys.stderr)
            sys.exit(1)

        try:
            # Initialize the Analyzer Service
            analyzer = AnalyzerService(source)
            app_details = analyzer.datasource.get_application(args.app_id)
            attempt = app_details["attempts"][0].get("attemptId", None)
            print("Attempt from application response >>", attempt)

            # Generate recommendations
            recommendation = analyzer.generate_recommendations(
                args.app_id, attempt, args.emr_id
            )

            # Print results to console
            print(
                f"{Colors.GREEN}{Colors.BOLD}Recommended Executor Memory: {recommendation.suggested_heap_in_gb}g"
            )
            print(
                f"Recommneded Executor Overhead Memory: {recommendation.suggested_overhead_in_gb}g"
            )
            print(
                f"{Colors.YELLOW}{Colors.BOLD}Current Executor Memory: {recommendation.additional_details['spark.executor.memory']}"
            )
            print(
                f"{Colors.YELLOW}{Colors.BOLD}Current Overhead Memory: {recommendation.additional_details.get('spark.executor.memoryOverhead', None) or recommendation.additional_details['spark.executor.memoryOverheadFactor']} {Colors.END}"
            )

            print(recommendation)

            # Save results to sink if path is provided
            if args.sink_path:
                sink = ParquetSink(args.sink_path)
                sink.save(recommendation)
            else:
                print("\n--sink-path not provided. Skipping save.")

        except Exception as e:
            print(f"{Colors.RED}Error: {e}{Colors.END}", file=sys.stderr)
            raise e
            # sys.exit(1)

    else:
        print("Unknown action", file=sys.stderr)


if __name__ == "__main__":
    main()
