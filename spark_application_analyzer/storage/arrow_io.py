"""Arrow/Parquet storage layer for metrics and recommendations.

This module handles data persistence and I/O operations:
- Conversion between dataclasses and PyArrow tables
- Parquet file generation for metrics storage
- Feather format support for fast I/O
- Data serialization/deserialization utilities
- Storage directory management and file organization
"""
import pandas as pd
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

# Sample data
# data = {"application_id": "application_1758077192353_0885", "emr_id": "j-3M8HDB16J7ZI7", "suggested_heap": 7238031000.1840315, "suggested_overhead": 2594560360.0659685, "buffer": 0.25, "current_p95_maxExecutors": 7, "avg_idle_pct": 9.333471161042997, "p95_idle_pct": 13.57751530460366, "recommended_maxExecutors": 7, "target_idle_pct": 15}
# data["metrics_collection_dt"] = datetime.now().date()
# data["app_name"] = "unique_appname_1"

# TODO: remove this hardcoded sink_location, should come from some config
def write_into_sink(data: dict, sink_location: str = 's3://bucket/key/'):
    # TODO: Update to support multiple sinks like JSON, CSV file, Iceberg Table etc.
    pyarrow_schema = pa.schema({
        pa.field("application_id", pa.string()),
        pa.field("emr_id", pa.string()),
        pa.field("suggested_heap_in_bytes", pa.float64()),
        pa.field("suggested_overhead_in_bytes", pa.float64()),
        pa.field("buffer", pa.float64()),
        pa.field("current_p95_maxExecutors", pa.int64()),
        pa.field("avg_idle_pct", pa.float64()),
        pa.field("p95_idle_pct", pa.float64()),
        pa.field("recommended_maxExecutors", pa.int64()),
        pa.field("target_idle_pct", pa.float64()),
        pa.field("metrics_collection_dt", pa.date64()),
        pa.field("app_name", pa.string()),
        pa.field("additional_details", pa.map_(pa.string(), pa.string()), nullable=True)
    })
    table_data = {key: [value] for key, value in data.items()}
    table = pa.Table.from_pydict(table_data, schema=pyarrow_schema)

    # Writes into S3 location with metrics_collection_dt as partitioned
    pq.write_to_dataset(
        table,
        root_path=sink_location,
        filesystem=s3fs.S3FileSystem(),
        partition_cols=["metrics_collection_dt"]
    )

# table_data = {key: [value] for key, value in data.items()}
# df = pd.DataFrame(table_data)

# # TODO: Update the path. maybe from configuration file?!?
# df.to_parquet(
#     path='s3://bucket-eu-west-1-978523670193-processed-data-s/transformation/rtd/srtd_control/spark_application_metrics/',
#     engine='pyarrow',
#     partition_cols=['metrics_collection_dt'],
#     compression='snappy',
# )