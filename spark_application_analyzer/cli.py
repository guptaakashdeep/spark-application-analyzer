# cli.py

import argparse
import sys
from collectors.spark_history import SparkHistoryServerClient
from utils.cli_colors import Colors
from utils.aws import get_history_server_url
from api import analyze_application


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

    # The 'list-apps' action is simple and doesn't use the main analysis workflow,
    # so it can have its own logic.
    if args.action == "list-apps":
        if args.emr_id:
            base_url = get_history_server_url(args.emr_id)
            print(f"Discovered Spark History Server URL via EMR: {base_url}")
        elif args.base_url:
            base_url = args.base_url
        else:
            base_url = "http://localhost:18080"
            print("Assuming local history server: http://localhost:18080")
        source = SparkHistoryServerClient(base_url)
        apps = source.list_applications()
        for app in apps:
            print(
                f"App ID: {app['id']}, Name: {app['name']}, Started: {app['attempts'][0]['startTime']}"
            )
        return

    if args.action == "get-recommendation":
        if not args.app_id:
            print("Error: --app-id required for get-recommendation", file=sys.stderr)
            sys.exit(1)

        try:
            # Delegate all core logic to the programmatic API
            print("--- Analyzing Application ---")
            recommendation = analyze_application(
                application_id=args.app_id,
                emr_id=args.emr_id,
                base_url=args.base_url,
                sink_path=args.sink_path,
            )

            # The CLI is now only responsible for presentation
            print("\n--- Recommendation Summary ---")
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
            print("\nFull recommendation object:")
            print(recommendation)
            if args.sink_path:
                print(f"\nRecommendation also saved to {args.sink_path}")

        except Exception as e:
            print(f"{Colors.RED}Error: {e}{Colors.END}", file=sys.stderr)
            raise e

    else:
        # This should not be reached given the 'choices' in argparse, but is here for safety.
        print(f"Unknown action: {args.action}", file=sys.stderr)


if __name__ == "__main__":
    main()
