# cli.py
import argparse
import sys
from datetime import datetime

import pyarrow.dataset as ds
import urllib3
from requests.exceptions import ReadTimeout
from spark_application_analyzer.api import analyze_application
from spark_application_analyzer.collectors.spark_history import SparkHistoryServerClient
from spark_application_analyzer.utils.aws import get_emrid
from spark_application_analyzer.utils.aws import get_history_server_url
from spark_application_analyzer.utils.aws import read_logs
from spark_application_analyzer.utils.cli_colors import Colors

urllib3.disable_warnings()


def _analyze_application(
    app_id: str, emr_id: str, base_url: str, sink_path: str, log_path: str
):
    try:
        # Delegate all core logic to the programmatic API
        print(f"{Colors.MAGENTA}--- Analyzing Application --- {Colors.END}")
        results = analyze_application(
            application_id=app_id,
            emr_id=emr_id,
            base_url=base_url,
            sink_path=sink_path,
            log_path=log_path,
        )

        recommendation = results.recommendation
        bottlenecks = results.bottlenecks

        # The CLI is now only responsible for presentation
        print("\n--- Recommendation Summary ---")
        print(
            f"{Colors.GREEN}{Colors.BOLD}Recommended Executor Memory: {recommendation.recommended_heap_memory_gb}g"
        )
        print(
            f"Recommneded Executor Overhead Memory: {recommendation.recommended_overhead_memory_gb}g"
        )
        print(
            f"{Colors.YELLOW}{Colors.BOLD}Current Executor Memory: {recommendation.current_configuration['spark.executor.memory']}"
        )
        print(
            f"{Colors.YELLOW}{Colors.BOLD}Current Overhead Memory: {recommendation.current_configuration.get('spark.executor.memoryOverhead', None) or recommendation.current_configuration['spark.executor.memoryOverheadFactor']} {Colors.END}"
        )
        # print("\nFull recommendation object:")
        # print(recommendation)
        print("\nBottleneck information:")
        print(bottlenecks)
        if sink_path:
            print(f"\Analysis results also saved to {sink_path}")

    except Exception as e:
        print(f"{Colors.RED}Error: {e}{Colors.END}", file=sys.stderr)
        if not isinstance(e, ValueError):
            raise e


def main():
    parser = argparse.ArgumentParser(description="Spark Executor Right Sizing CLI")
    parser.add_argument(
        "--emr-id", type=str, help="EMR Cluster ID (to fetch history server endpoint)"
    )
    parser.add_argument("--base-url", type=str, help="Spark History Server base URL")
    parser.add_argument(
        "--app-id", type=str, help="Spark Application ID (for get-recommendation)"
    )
    parser.add_argument(
        "--all-apps",
        action="store_true",
        help="Process all the applications that are completed.",
    )
    # TODO: Move this to configs?
    parser.add_argument(
        "--sink-path",
        type=str,
        help="Path to save recommendation parquet file (e.g., s3://bucket/path)",
    )
    parser.add_argument(
        "--log-path",
        type=str,
        help="Path to save metrics execution logs (e.g., s3://bucket/path)",
    )
    parser.add_argument(
        "--on-emr",
        action="store_true",
        help="Flag to identify if cli is running on EMR master node to identify emr-id by itself",
    )

    args = parser.parse_args()
    apps_list = []
    base_url = args.base_url

    if not args.app_id and not args.all_apps:
        print(
            "Error: --app-id required for recommendations and bottlenec identification",
            file=sys.stderr,
        )
        sys.exit(1)

    if args.on_emr and not args.emr_id:
        emr_id = get_emrid()
        print(f"Fetched emr-id: {emr_id}")
    else:
        emr_id = args.emr_id
        print(f"Provided emr-d: {emr_id}")

    if args.all_apps:
        if emr_id:
            base_url = get_history_server_url(emr_id)
            print(f"Discovered Spark History Server URL via EMR: {base_url}")
        elif args.base_url:
            base_url = args.base_url
        else:
            base_url = "http://localhost:18080"
        source = SparkHistoryServerClient(base_url)
        apps = source.list_completed_applications()
        # Add a logic for removing already processed application from logging table. if log_path present.
        if apps:
            app_ids = [app["id"] for app in apps]
            logs_dataset = read_logs(args.log_path)
            # Get list of all the processed applications today
            if logs_dataset:
                processed_apps = (
                    logs_dataset.to_table(
                        columns=["application_id"],
                        filter=(
                            ds.field("processed_date") == str(datetime.now().date())
                        ),
                    )
                    .group_by(["application_id"])
                    .aggregate([])
                    .column("application_id")
                    .to_pylist()
                )
                apps_list = list(set(app_ids) - set(processed_apps))
                print(f"Total Apps Num: {len(app_ids)}")
                print(f"Processed Apps: {len(processed_apps)}")
                print(f"Apps to collect metrics: {len(apps_list)}")
            else:
                print(f"Apps to collect metrics: {len(app_ids)}")
                apps_list = app_ids
    else:
        apps_list.append(args.app_id)

    # Delegate all core logic to the programmatic API
    for app_id in apps_list:
        print(
            f"{Colors.CYAN}{Colors.BOLD}==== Analyzing Application: {app_id} ==== {Colors.END}"
        )
        try:
            _analyze_application(
                app_id=app_id,
                emr_id=emr_id,
                base_url=base_url,
                sink_path=args.sink_path,
                log_path=args.log_path,
            )
            print(
                f"{Colors.CYAN}{Colors.BOLD}==== Analysis Completed for Application: {app_id} ==== {Colors.END}"
            )
        except ReadTimeout:
            print(
                f"{Colors.RED}{Colors.BOLD}==== Read timed out for Application: {app_id} ==== {Colors.END}"
            )
        except Exception as e:
            raise e


if __name__ == "__main__":
    main()
