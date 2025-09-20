import boto3


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
