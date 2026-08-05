"""Bronze-to-Silver historical events and derived current-state records."""

from __future__ import annotations

import logging

from delta.tables import DeltaTable
from openlineage.client.run import RunState
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from capstone_de.lakehouse.paths import LakehousePaths
from capstone_de.lakehouse.spark import create_spark_session
from capstone_de.lineage.emitter import LineageEmitter
from capstone_de.settings import Settings

LOGGER = logging.getLogger(__name__)


def transform_bronze_to_silver(bronze: DataFrame) -> DataFrame:
    """Return one deterministic historical record per immutable event ID."""
    event_window = Window.partitionBy("event_id").orderBy(
        col("ingested_at").desc(),
        col("kafka_timestamp").desc(),
        col("kafka_partition").desc(),
        col("kafka_offset").desc(),
    )
    return (
        bronze.withColumn("event_rank", row_number().over(event_window))
        .filter(col("event_rank") == 1)
        .drop("event_rank")
    )


def transform_history_to_current(history: DataFrame) -> DataFrame:
    """Derive the latest event for each customer and event type from full history."""
    current_window = Window.partitionBy("customer_id", "event_type").orderBy(
        col("occurred_at").desc(),
        col("ingested_at").desc(),
        col("kafka_timestamp").desc(),
        col("kafka_partition").desc(),
        col("kafka_offset").desc(),
        col("event_id").desc(),
    )
    return (
        history.withColumn("current_rank", row_number().over(current_window))
        .filter(col("current_rank") == 1)
        .drop("current_rank")
    )


def merge_silver(settings: Settings, lineage: LineageEmitter) -> None:
    """Merge deduplicated event history into Silver using immutable event IDs."""
    paths = LakehousePaths.from_settings(settings)
    run_id = lineage.new_run_id()
    lineage.emit(
        RunState.START, "silver_history_merge", run_id, [paths.bronze], [paths.silver]
    )
    spark = create_spark_session(settings, "silver-history-merge")
    try:
        source = transform_bronze_to_silver(spark.read.format("delta").load(paths.bronze))
        if DeltaTable.isDeltaTable(spark, paths.silver):
            target = DeltaTable.forPath(spark, paths.silver)
            (
                target.alias("target")
                .merge(source.alias("source"), "target.event_id = source.event_id")
                .whenMatchedUpdateAll(condition="source.ingested_at >= target.ingested_at")
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            source.write.format("delta").mode("errorifexists").option("mergeSchema", "false").save(
                paths.silver
            )
    except Exception:
        lineage.emit(
            RunState.FAIL, "silver_history_merge", run_id, [paths.bronze], [paths.silver]
        )
        LOGGER.exception("Silver historical MERGE failed")
        raise
    lineage.emit(
        RunState.COMPLETE, "silver_history_merge", run_id, [paths.bronze], [paths.silver]
    )
    LOGGER.info("Silver historical MERGE completed", extra={"path": paths.silver})


def build_silver_current(settings: Settings, lineage: LineageEmitter) -> None:
    """Materialize the optional latest-state view without altering event history."""
    paths = LakehousePaths.from_settings(settings)
    run_id = lineage.new_run_id()
    lineage.emit(
        RunState.START,
        "silver_current_snapshot",
        run_id,
        [paths.silver],
        [paths.silver_current],
    )
    spark = create_spark_session(settings, "silver-current-snapshot")
    try:
        history = spark.read.format("delta").load(paths.silver)
        current = transform_history_to_current(history)
        current.write.format("delta").mode("overwrite").option("overwriteSchema", "false").save(
            paths.silver_current
        )
    except Exception:
        lineage.emit(
            RunState.FAIL,
            "silver_current_snapshot",
            run_id,
            [paths.silver],
            [paths.silver_current],
        )
        LOGGER.exception("Silver current-state snapshot failed")
        raise
    lineage.emit(
        RunState.COMPLETE,
        "silver_current_snapshot",
        run_id,
        [paths.silver],
        [paths.silver_current],
    )
    LOGGER.info("Silver current-state snapshot completed", extra={"path": paths.silver_current})
