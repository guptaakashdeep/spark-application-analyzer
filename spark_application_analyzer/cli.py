# cli.py
import argparse
import sys
from datetime import datetime

import pyarrow.dataset as ds
import urllib3
from requests.exceptions import ReadTimeout
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from spark_application_analyzer.api import analyze_application
from spark_application_analyzer.collectors.spark_history import SparkHistoryServerClient
from spark_application_analyzer.utils.aws import get_emrid
from spark_application_analyzer.utils.aws import get_history_server_url
from spark_application_analyzer.utils.aws import read_logs

urllib3.disable_warnings()

console = Console()


def _analyze_application(
    app_id: str, emr_id: str, base_url: str, sink_path: str, log_path: str
):
    try:
        # Delegate all core logic to the programmatic API
        console.rule(f"[bold magenta]Analyzing Application: {app_id}[/]")
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

        # Create a table for recommendation summary
        rec_table = Table(title="Recommendation Summary", show_header=False, box=None)
        rec_table.add_column("Metric", style="cyan bold")
        rec_table.add_column("Value", style="green")

        rec_table.add_row(
            "Recommended Executor Memory",
            f"{recommendation.recommended_heap_memory_gb}g",
        )
        rec_table.add_row(
            "Recommended Executor Overhead Memory",
            f"{recommendation.recommended_overhead_memory_gb}g",
        )

        current_overhead = (
            recommendation.current_configuration.get(
                "spark.executor.memoryOverhead", None
            )
            or recommendation.current_configuration[
                "spark.executor.memoryOverheadFactor"
            ]
        )

        rec_table.add_row(
            "Current Executor Memory",
            f"[yellow]{recommendation.current_configuration['spark.executor.memory']}[/]",
        )
        rec_table.add_row("Current Overhead Memory", f"[yellow]{current_overhead}[/]")

        console.print(Panel(rec_table, expand=False, border_style="green"))

        if bottlenecks:
            console.print("\n[bold red]Bottleneck Information:[/]")

            # Summary Metrics
            grid = Table.grid(expand=True)
            grid.add_column()
            grid.add_column(justify="right")
            grid.add_row(
                "Jobs:",
                f"{bottlenecks.num_jobs} (Failed: [red]{bottlenecks.num_failed_jobs}[/])",
            )
            grid.add_row(
                "Stages:",
                f"{bottlenecks.num_stages} (Failed: [red]{bottlenecks.num_failed_stages}[/])",
            )
            grid.add_row("GC Pressure Ratio:", f"{bottlenecks.gc_pressure_ratio:.2f}")
            console.print(Panel(grid, title="Overview", border_style="red"))

            # Slowest Jobs
            if bottlenecks.slowest_jobs:
                job_table = Table(
                    title="Slowest Jobs", show_header=True, header_style="bold magenta"
                )
                job_table.add_column("Job ID", style="cyan")
                job_table.add_column("Name")
                job_table.add_column("Duration (s)", justify="right")
                job_table.add_column("Stages", justify="right")
                job_table.add_column("Tasks", justify="right")

                for job in bottlenecks.slowest_jobs:
                    # Handle both object and dict access
                    job_id = (
                        str(job.get("job_id"))
                        if isinstance(job, dict)
                        else str(job.job_id)
                    )
                    name = job.get("name") if isinstance(job, dict) else job.name
                    duration = (
                        job.get("duration_secs")
                        if isinstance(job, dict)
                        else job.duration_secs
                    )
                    stages = (
                        str(job.get("stages_count"))
                        if isinstance(job, dict)
                        else str(job.stages_count)
                    )
                    task_count = (
                        job.get("task_count")
                        if isinstance(job, dict)
                        else job.task_count
                    )
                    failed_tasks = (
                        job.get("failed_tasks")
                        if isinstance(job, dict)
                        else job.failed_tasks
                    )

                    job_table.add_row(
                        job_id,
                        name,
                        f"{duration:.2f}",
                        stages,
                        f"{task_count} (Failed: {failed_tasks})",
                    )
                console.print(job_table)

            # Slowest Stages
            if bottlenecks.slowest_stages:
                stage_table = Table(
                    title="Slowest Stages",
                    show_header=True,
                    header_style="bold magenta",
                )
                stage_table.add_column("Stage ID", style="cyan")
                stage_table.add_column("Name")
                stage_table.add_column("Duration (s)", justify="right")
                stage_table.add_column("Tasks", justify="right")

                for stage in bottlenecks.slowest_stages:
                    # Handle both object and dict access
                    stage_id = (
                        str(stage.get("stage_id"))
                        if isinstance(stage, dict)
                        else str(stage.stage_id)
                    )
                    name = stage.get("name") if isinstance(stage, dict) else stage.name
                    duration = (
                        stage.get("duration_secs")
                        if isinstance(stage, dict)
                        else stage.duration_secs
                    )
                    task_count = (
                        stage.get("task_count")
                        if isinstance(stage, dict)
                        else stage.task_count
                    )
                    failed_tasks = (
                        stage.get("failed_tasks")
                        if isinstance(stage, dict)
                        else stage.failed_tasks
                    )
                    killed_tasks = (
                        stage.get("killed_tasks")
                        if isinstance(stage, dict)
                        else stage.killed_tasks
                    )

                    stage_table.add_row(
                        stage_id,
                        name,
                        f"{duration:.2f}",
                        f"{task_count} (Failed: {failed_tasks}, Killed: {killed_tasks})",
                    )
                console.print(stage_table)

            # High Spill Stages
            if bottlenecks.high_spill_stages:
                spill_table = Table(
                    title="High Spill Stages",
                    show_header=True,
                    header_style="bold yellow",
                )
                spill_table.add_column("Stage ID", style="cyan")
                spill_table.add_column("Name")
                spill_table.add_column("Memory Spill (MB)", justify="right")
                spill_table.add_column("Disk Spill (MB)", justify="right")

                for stage in bottlenecks.high_spill_stages:
                    # Handle both object and dict access
                    stage_id = (
                        str(stage.get("stage_id"))
                        if isinstance(stage, dict)
                        else str(stage.stage_id)
                    )
                    name = stage.get("name") if isinstance(stage, dict) else stage.name
                    mem_spill = (
                        stage.get("memory_spilled_mb")
                        if isinstance(stage, dict)
                        else stage.memory_spilled_mb
                    )
                    disk_spill = (
                        stage.get("disk_spilled_mb")
                        if isinstance(stage, dict)
                        else stage.disk_spilled_mb
                    )

                    spill_table.add_row(
                        stage_id, name, f"{mem_spill:.2f}", f"{disk_spill:.2f}"
                    )
                console.print(spill_table)

            # Stage Retries
            if bottlenecks.stage_retries:
                retry_table = Table(
                    title="Stage Retries", show_header=True, header_style="bold red"
                )
                retry_table.add_column("Stage ID", style="cyan")
                retry_table.add_column("Retry Count", justify="right", style="red")

                # Handle both object and dict access
                # Based on analytics/bottleneck.py, this is likely a dict {stage_id: count}
                # But if it comes from the dataclass default, it might be a StageRetries object (unlikely given the code)
                # Or if it's passed as a dict from the API.

                retries_data = bottlenecks.stage_retries
                if isinstance(retries_data, dict):
                    for stage_id, count in retries_data.items():
                        retry_table.add_row(str(stage_id), str(count))
                elif hasattr(
                    retries_data, "stage_id"
                ):  # Single object case (unlikely but per type hint)
                    retry_table.add_row(
                        str(retries_data.stage_id), str(retries_data.num_retries)
                    )

                console.print(retry_table)

    except Exception as e:
        from rich.markup import escape

        console.print(f"[bold red]Error:[/bold red] {escape(str(e))}", style="red")
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
        console.print(
            "[bold red]Error: --app-id required for recommendations and bottleneck identification[/]",
        )
        sys.exit(1)

    if args.on_emr and not args.emr_id:
        emr_id = get_emrid()
        console.print(f"[blue]Fetched emr-id: {emr_id}[/]")
    else:
        emr_id = args.emr_id
        if emr_id:
            console.print(f"[blue]Provided emr-id: {emr_id}[/]")

    if args.all_apps:
        if emr_id:
            base_url = get_history_server_url(emr_id)
            console.print(
                f"[blue]Discovered Spark History Server URL via EMR: {base_url}[/]"
            )
        elif args.base_url:
            base_url = args.base_url
        else:
            base_url = "http://localhost:18080"

        with console.status("[bold green]Fetching application list..."):
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

                stats_table = Table(show_header=False, box=None)
                stats_table.add_row("Total Apps Num", str(len(app_ids)))
                stats_table.add_row("Processed Apps", str(len(processed_apps)))
                stats_table.add_row(
                    "Apps to collect metrics", str(len(apps_list)), style="bold yellow"
                )
                console.print(stats_table)
            else:
                console.print(f"Apps to collect metrics: {len(app_ids)}")
                apps_list = app_ids
    else:
        apps_list.append(args.app_id)

    # Delegate all core logic to the programmatic API
    for app_id in apps_list:
        try:
            _analyze_application(
                app_id=app_id,
                emr_id=emr_id,
                base_url=base_url,
                sink_path=args.sink_path,
                log_path=args.log_path,
            )
            console.print(
                f"[bold cyan]==== Analysis Completed for Application: {app_id} ====[/]"
            )
        except ReadTimeout:
            console.print(
                f"[bold red]==== Read timed out for Application: {app_id} ====[/]"
            )
        except Exception as e:
            raise e


if __name__ == "__main__":
    main()
