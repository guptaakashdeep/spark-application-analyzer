CREATE EXTERNAL TABLE `control_db.spark_application_metrics`(
  `application_id` string,
  `emr_id` string,
  `app_name` string,
  `time_taken_mins` double,
  `num_jobs` int,
  `num_stages` int,
  `num_failed_jobs` int,
  `num_failed_stages` int,
  `recommended_heap_bytes` double,
  `recommended_overhead_bytes` double,
  `recommended_heap_gb` int,
  `recommended_overhead_gb` int,
  `buffer` double,
  `avg_idle_pct` double,
  `target_idle_pct` double,
  `current_p95_maxExecutors` bigint,
  `recommended_maxExecutors` bigint,
  `current_configuration` map<string,string>,
  `gc_pressure_ratio` double,
  `high_spill_stages` array<struct<
        stage_id: int,
        memory_spilled_mb: double,
        disk_spilled_mb: double,
        name: string
    >>,
  `slowest_jobs` array<struct<
        job_id: int,
        name: string,
        duration_secs: double,
        stages_count: int,
        task_count: int,
        failed_tasks: int
  >>,
  `slowest_stages` array<struct<
        stage_id: int,
        name: string,
        duration_secs: double,
        task_count: int,
        failed_tasks: int,
        killed_tasks: int
  >>
  )
PARTITIONED BY (
  `metrics_collection_dt` date)
STORED AS PARQUET
LOCATION
  's3://data_bucket/blogs/control_db/spark_application_metrics'
