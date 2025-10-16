"""Arrow/Parquet storage layer for metrics and recommendations.

This module handles data persistence and I/O operations, starting with Parquet.
"""
from dataclasses import asdict

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from spark_application_analyzer.models.recommendation import AnalysisResults

from .datasink import IDataSink


class ParquetSink(IDataSink):
    """
    A data sink that writes recommendation data to a Parquet dataset.
    """

    def __init__(self, sink_location: str):
        """
        Initializes the sink with a target location.
        :param sink_location: The root path for the Parquet dataset (e.g., 's3://my-bucket/my-path/').
        """
        self.sink_location = sink_location.rstrip("/")
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
                pa.field("metrics_collection_dt", pa.date32()),
                pa.field("emr_id", pa.string()),
                pa.field("recommended_heap_bytes", pa.float64()),
                pa.field("recommended_overhead_bytes", pa.float64()),
                pa.field("recommended_heap_gb", pa.int64()),
                pa.field("recommended_overhead_gb", pa.int64()),
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
        print("Table Data: ", table_data)
        table = pa.Table.from_pydict(table_data, schema=pyarrow_schema)

        print(f"Writing recommendations to {self.sink_location}")
        pq.write_to_dataset(
            table,
            root_path=self.sink_location,
            filesystem=self.filesystem,
            partition_cols=["metrics_collection_dt"],
            existing_data_behavior="overwrite_or_ignore",
        )
