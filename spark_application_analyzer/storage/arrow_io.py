"""Arrow/Parquet storage layer for metrics and recommendations.

This module handles data persistence and I/O operations, starting with Parquet.
"""
from dataclasses import asdict
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from spark_application_analyzer.models.recommendation import AnalysisResults

from .datasink import IDataSink


class ParquetSink(IDataSink):
    """
    A data sink that writes recommendation data to a Parquet dataset.
    """

    def __init__(self, sink_location: str, log_location: Optional[str] = None):
        """
        Initializes the sink with a target location.
        :param sink_location: The root path for the Parquet dataset (e.g., 's3://my-bucket/my-path/').
        :param log_location: The root path for the Parquet logs.
        """
        self.sink_location = sink_location.rstrip("/")
        self.log_location = log_location.rstrip("/") if log_location else ""
        self.filesystem = (
            s3fs.S3FileSystem() if self.sink_location.startswith("s3://") else None
        )

    def _get_slowest_jobs_schema(self):
        return pa.struct(
            [
                ("job_id", pa.int32()),
                ("name", pa.string()),
                ("duration_secs", pa.float64()),
                ("stages_count", pa.int32()),
                ("task_count", pa.int32()),
                ("failed_tasks", pa.int32()),
            ]
        )

    def _get_slowest_stages_schema(self):
        return pa.struct(
            [
                ("stage_id", pa.int32()),
                ("name", pa.string()),
                ("duration_secs", pa.float64()),
                ("task_count", pa.int32()),
                ("failed_tasks", pa.int32()),
                ("killed_tasks", pa.int32()),
            ]
        )

    def _get_high_spill_stages_schema(self):
        return pa.struct(
            [
                ("stage_id", pa.int32()),
                ("name", pa.string()),
                ("memory_spilled_mb", pa.float64()),
                ("disk_spilled_mb", pa.float64()),
            ]
        )

    def save(self, data: AnalysisResults) -> None:
        """
        Saves the recommendation data to a partitioned Parquet dataset.
        The dataset is partitioned by 'metrics_collection_dt'.
        """
        if not isinstance(data, AnalysisResults):
            raise TypeError("Data must be a Recommendation object")

        pyarrow_schema = pa.schema(
            [
                pa.field("application_id", pa.string()),
                pa.field("app_name", pa.string()),
                pa.field("time_taken_mins", pa.float64()),
                pa.field("num_jobs", pa.int32()),
                pa.field("num_stages", pa.int32()),
                pa.field("num_failed_jobs", pa.int32()),
                pa.field("num_failed_stages", pa.int32()),
                pa.field("metrics_collection_dt", pa.date32()),
                pa.field("emr_id", pa.string()),
                pa.field("max_heap_memory", pa.int64()),
                pa.field("max_heap_memory_gb", pa.int64()),
                pa.field("max_total_memory", pa.int64()),
                pa.field("max_total_memory_gb", pa.int64()),
                pa.field("max_overhead_memory", pa.int64()),
                pa.field("max_overhead_memory_gb", pa.int64()),
                pa.field("recommended_heap_bytes", pa.float64()),
                pa.field("recommended_overhead_bytes", pa.float64()),
                pa.field("recommended_heap_memory_gb", pa.int64()),
                pa.field("recommended_overhead_memory_gb", pa.int64()),
                pa.field("buffer", pa.float64()),
                pa.field("current_p95_maxExecutors", pa.int64()),
                pa.field("avg_idle_pct", pa.float64()),
                pa.field("recommended_maxExecutors", pa.int64()),
                pa.field("target_idle_pct", pa.float64()),
                pa.field(
                    "current_configuration",
                    pa.map_(pa.string(), pa.string()),
                    nullable=True,
                ),
                pa.field("gc_pressure_ratio", pa.float64()),
                pa.field(
                    "stage_retries",
                    pa.map_(pa.int32(), pa.int32()),
                    nullable=True,
                ),
                pa.field(
                    "high_spill_stages",
                    pa.list_(self._get_high_spill_stages_schema()),
                    nullable=True,
                ),
                pa.field(
                    "slowest_jobs",
                    pa.list_(self._get_slowest_jobs_schema()),
                    nullable=True,
                ),
                pa.field(
                    "slowest_stages",
                    pa.list_(self._get_slowest_stages_schema()),
                    nullable=True,
                ),
            ]
        )

        recommendation_data = {
            key: [value] for key, value in asdict(data.recommendation).items()
        }
        bottleneck_data = {
            key: [value] for key, value in asdict(data.bottlenecks).items()
        }
        table_data = {**recommendation_data, **bottleneck_data}
        # print("Table Data: ", table_data)
        table = pa.Table.from_pydict(table_data, schema=pyarrow_schema)

        print(f"Writing recommendations to {self.sink_location}")
        pq.write_to_dataset(
            table,
            root_path=self.sink_location,
            filesystem=self.filesystem,
            partition_cols=["metrics_collection_dt"],
            existing_data_behavior="overwrite_or_ignore",
        )

    def log(self, log_data: dict) -> None:
        """Log the application analysis status"""
        if not self.log_location:
            return

        log_schema = pa.schema(
            [
                pa.field("application_id", pa.string()),
                pa.field("app_name", pa.string()),
                pa.field("completion_timestamp", pa.string()),
                pa.field("first_seen_timestamp", pa.timestamp("ms")),
                pa.field("processed_timestamp", pa.timestamp("ms")),
                pa.field("processing_status", pa.string()),
                pa.field("failure_reason", pa.string(), nullable=True),
                pa.field("processed_date", pa.date32()),
            ]
        )

        log_table_data = {key: [value] for key, value in log_data.items()}

        table = pa.Table.from_pydict(log_table_data, schema=log_schema)
        print(f"Writing processing logs to {self.log_location}")
        pq.write_to_dataset(
            table,
            root_path=self.log_location,
            filesystem=self.filesystem,
            partition_cols=["processed_date"],
            existing_data_behavior="overwrite_or_ignore",
        )
