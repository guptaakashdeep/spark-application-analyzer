CREATE EXTERNAL TABLE `control_db.metrics_run_logs`(
    `application_id` string,
    `app_name` string,
    `completion_timestamp` string,
    `first_seen_timestamp` timestamp,
    `processed_timestamp` timestamp,
    `processing_status` string,
    `failure_reason` string)
PARTITIONED BY (
    `processed_date` date)
STORED AS PARQUET
LOCATION
    's3://data_bucket/blogs/control_db/metrics_run_logs'