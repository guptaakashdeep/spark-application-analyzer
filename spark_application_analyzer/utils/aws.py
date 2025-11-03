import json
import shlex
import subprocess as sp
from typing import Optional

import boto3
import pyarrow.dataset as ds


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


# TODO: Maybe move to somewhere else?
def read_logs(log_location) -> Optional[ds.dataset]:
    """Read processed_configruation logs from the Parquet dataset."""
    if not log_location:
        return None

    try:
        dataset = ds.dataset(log_location, format="parquet", partitioning="hive")
        return dataset
    except Exception as e:
        print(f"Error reading logs from {log_location}: {e}")
        raise e


def get_emrid():
    """Gets EMR ID if app is running on EMR master node"""
    try:
        emr_result = sp.run(
            shlex.split("cat /mnt/var/lib/info/job-flow.json"),
            shell=False,
            capture_output=True,
            text=True,
        )
        if emr_result.returncode != 0:
            raise Exception("Error occured while getting emr_id.")
        else:
            result = emr_result.stdout.strip()
            json_emr_id = json.loads(result)
            emr_id = json_emr_id.get("jobFlowId")
            return emr_id
    except Exception as e:
        raise ValueError("Issues fetching EMR ID on master", str(e))
