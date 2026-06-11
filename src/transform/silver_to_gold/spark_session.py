from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession


def log_step(message: str) -> None:
    print(f"[silver_to_gold] {message}")


def create_spark() -> SparkSession:
    warehouse_dir = Path("spark-warehouse").resolve().as_uri()
    master = os.getenv("SPARK_MASTER", "local[2]")
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "4g")
    shuffle_partitions = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")
    default_parallelism = os.getenv("SPARK_DEFAULT_PARALLELISM", shuffle_partitions)
    max_result_size = os.getenv("SPARK_DRIVER_MAX_RESULT_SIZE", "1g")
    event_log_enabled_raw = os.getenv("SPARK_EVENTLOG_ENABLED", "false")
    event_log_enabled = event_log_enabled_raw.lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    event_log_dir = os.getenv("SPARK_EVENTLOG_DIR", "/tmp/spark-events")

    builder = (
        SparkSession.builder.appName("SilverToGoldRealEstate")
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.maxResultSize", max_result_size)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.default.parallelism", default_parallelism)
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    )

    print(
        "[spark_session] SPARK_EVENTLOG_ENABLED=",
        os.getenv("SPARK_EVENTLOG_ENABLED"),
        "->",
        event_log_enabled,
        "event_log_dir=",
        event_log_dir,
    )

    if event_log_enabled:
        builder = builder.config("spark.eventLog.enabled", "true").config(
            "spark.eventLog.dir", event_log_dir
        )


    return builder.getOrCreate()
