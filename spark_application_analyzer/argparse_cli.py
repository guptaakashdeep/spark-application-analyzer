# cli.py

import argparse
import sys
import boto3
from datetime import datetime
from collectors.spark_history import SparkHistoryServerClient
from storage.arrow_io import write_into_sink
from utils.cli_colors import Colors


def get_history_server_url(emr_id: str) -> str:
    """Fetch Spark History Server URL from EMR cluster using boto3."""
    client = boto3.client("emr")
    cluster = client.describe_cluster(ClusterId=emr_id)
    apps = cluster["Cluster"]["Applications"]
    # Try common logic for Spark 3.x/EMR 7.x
    history_server_dns = None
    for app in apps:
        if app["Name"].lower() == "spark":
            # A common pattern; adjust this if DNS field is different for your EMR version
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

    args = parser.parse_args()

    # Determine Spark History Server endpoint
    if args.emr_id:
        base_url = get_history_server_url(args.emr_id)
        print(f"Discovered Spark History Server URL via EMR: {base_url}")
    elif args.base_url:
        base_url = args.base_url
    else:
        # Assume history server is local (common on EMR master)
        base_url = "http://localhost:18080"
        print("Assuming local history server: http://localhost:18080")

    # Initialize client
    client = SparkHistoryServerClient(base_url)

    # CLI Actions
    if args.action == "list-apps":
        apps = client.list_applications()
        for app in apps:
            print(
                f"App ID: {app['id']}, Name: {app['name']}, Started: {app['attempts'][0]['startTime']}"
            )
    elif args.action == "get-recommendation":
        if not args.app_id:
            print("Error: --app-id required for get-recommendation", file=sys.stderr)
            sys.exit(1)
        # TODO: maybe move this to somewhere else to keep this clean?
        # check if application id exists
        app_id = args.app_id
        apps = client.list_applications()
        app_exists = app_id in [app_detail["id"] for app_detail in apps]
        if not app_exists:
            # TODO: Maybe defer this to be checked later? App_ids are not present immediately in SHS.
            raise Exception(
                f"{app_id} not found in Spark History Server. Check in sometime or validate app_id"
            )
        # TODO: Check here if the application run is completed...
        app_details = client.get_application(app_id)
        # ---
        # TODO: Check whether attempts are there in application
        attempt = app_details["attempts"][0].get("attemptId", None)

        # Based on this URLs changes: whether attemptId will be taken into account or not
        input_params = {
            "metrics_collection_dt": datetime.now().date(),
            "application_id": app_id,
            "emr_id": args.emr_id,
            "app_name": app_details["name"]
        }
        app_env = client.get_environment(app_id, attempt)
        env_parameters = {
            "spark.executor.instances",
            "spark.executor.memory",
            "spark.dynamicAllocation.enabled",
            "spark.dynamicAllocation.minExecutors",
            "spark.dynamicAllocation.maxExecutors",
            "spark.executor.cores",
            "spark.executor.memoryOverhead",
            "spark.executor.memoryOverheadFactor",
            "spark.executor.processTreeMetrics.enabled"
        }
        defined_exec_params = {
            env[0]: env[1]
            for env in list(
                filter(
                    lambda prop: prop[0] in env_parameters, app_env["sparkProperties"]
                )
            )
        }
        can_recommend = defined_exec_params.get("spark.executor.processTreeMetrics.enabled", False)
        if not can_recommend:
            print(f"{Colors.RED}{Colors.BOLD} Recommendations cannot be determined.Spark Application must run with configuration spark.executor.processTreeMetrics.enabled=true {Colors.END}")
            sys.exit(0)
        recommendations = client.get_recommended_executor_size(app_id, attempt)
        exec_num_recommendations = client.get_recommended_executor_numbers(app_id, attempt)
        print(
            f"{Colors.GREEN}{Colors.BOLD}Recommended Executor Memory: {recommendations['suggested_heap_in_bytes'] / 1024**3}g"
        )
        print(
            f"Recommneded Executor Overhead Memory: {recommendations['suggested_overhead_in_bytes'] / 1024**3}g"
        )
        print(
            f"{Colors.YELLOW}{Colors.BOLD}Current Executor Memory: {defined_exec_params['spark.executor.memory']}"
        )
        print(
            f"{Colors.YELLOW}{Colors.BOLD}Current Overhead Memory: {defined_exec_params.get('spark.executor.memoryOverhead', None) or defined_exec_params['spark.executor.memoryOverheadFactor']} {Colors.END}"
        )
        full_recommendations = {"additional_details": defined_exec_params,**recommendations, **exec_num_recommendations, **input_params}
        # TODO: Add sink_location -- should be defined via some config?!?!
        write_into_sink(full_recommendations)
    else:
        print("Unknown action", file=sys.stderr)


if __name__ == "__main__":
    main()
