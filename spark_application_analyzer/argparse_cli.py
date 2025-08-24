# cli.py

import argparse
import sys
import boto3
from spark_application_analyzer.collectors.spark_history import SparkHistoryServerClient


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
        choices=["list-apps", "get-metrics"],
        help="CLI Action",
    )
    parser.add_argument(
        "--app-id", type=str, help="Spark Application ID (for get-metrics)"
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
    elif args.action == "get-metrics":
        if not args.app_id:
            print("Error: --app-id required for get-metrics", file=sys.stderr)
            sys.exit(1)
        metrics = client.get_executor_metrics(args.app_id)
        for m in metrics:
            print(m)
    else:
        print("Unknown action", file=sys.stderr)


if __name__ == "__main__":
    main()
