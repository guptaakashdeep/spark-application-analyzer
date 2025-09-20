"""Arrow/Parquet storage layer for metrics and recommendations.

This module handles data persistence and I/O operations, starting with Parquet.
"""

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from dataclasses import asdict

from models.recommendation import Recommendation
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

    def save(self, data: Recommendation) -> None:
        """
        Saves the recommendation data to a partitioned Parquet dataset.
        The dataset is partitioned by 'metrics_collection_dt'.
        """
        if not isinstance(data, Recommendation):
            raise TypeError("Data must be a Recommendation object")

        pyarrow_schema = pa.schema(
            [
                pa.field("application_id", pa.string()),
                pa.field("app_name", pa.string()),
                pa.field("metrics_collection_dt", pa.date32()),
                pa.field("emr_id", pa.string()),
                pa.field("suggested_heap_in_bytes", pa.float64()),
                pa.field("suggested_overhead_in_bytes", pa.float64()),
                pa.field("suggested_heap_in_gb", pa.int64()),
                pa.field("suggested_overhead_in_gb", pa.int64()),
                pa.field("buffer", pa.float64()),
                pa.field("current_p95_maxExecutors", pa.int64()),
                pa.field("avg_idle_pct", pa.float64()),
                pa.field("p95_idle_pct", pa.float64()),
                pa.field("recommended_maxExecutors", pa.int64()),
                pa.field("target_idle_pct", pa.float64()),
                pa.field(
                    "additional_details",
                    pa.map_(pa.string(), pa.string()),
                    nullable=True,
                ),
            ]
        )

        table_data = {key: [value] for key, value in asdict(data).items()}
        table = pa.Table.from_pydict(table_data, schema=pyarrow_schema)

        print(f"Writing recommendations to {self.sink_location}")
        pq.write_to_dataset(
            table,
            root_path=self.sink_location,
            filesystem=self.filesystem,
            partition_cols=["metrics_collection_dt"],
            existing_data_behavior="overwrite_or_ignore",
        )
