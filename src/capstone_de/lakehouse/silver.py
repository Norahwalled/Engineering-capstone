"""Bronze-to-Silver Delta transformation with business-keyed ACID upserts."""

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
    """Deduplicate by immutable event ID and retain the newest event per business key."""
    event_window = Window.partitionBy("event_id").orderBy(col("ingested_at").desc())
    customer_window = Window.partitionBy("customer_id", "event_type").orderBy(
        col("occurred_at").desc()
    )
    return (
        bronze.dropDuplicates(["event_id"])
        .withColumn("event_rank", row_number().over(event_window))
        .filter(col("event_rank") == 1)
        .drop("event_rank")
        .withColumn("business_rank", row_number().over(customer_window))
        .filter(col("business_rank") == 1)
        .drop("business_rank")
    )


def merge_silver(settings: Settings, lineage: LineageEmitter) -> None:
    """Perform a real Delta Lake MERGE keyed on customer ID and event type."""
    paths = LakehousePaths.from_settings(settings)
    run_id = lineage.new_run_id()
    lineage.emit(RunState.START, "silver_merge", run_id, [paths.bronze], [paths.silver])
    spark = create_spark_session(settings, "silver-merge")
    try:
        source = transform_bronze_to_silver(spark.read.format("delta").load(paths.bronze))
        if DeltaTable.isDeltaTable(spark, paths.silver):
            target = DeltaTable.forPath(spark, paths.silver)
            merge_condition = (
                "target.customer_id = source.customer_id AND target.event_type = source.event_type"
            )
            (
                target.alias("target")
                .merge(source.alias("source"), merge_condition)
                .whenMatchedUpdateAll(condition="source.occurred_at >= target.occurred_at")
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            source.write.format("delta").mode("errorifexists").option("mergeSchema", "false").save(
                paths.silver
            )
    except Exception:
        lineage.emit(RunState.FAIL, "silver_merge", run_id, [paths.bronze], [paths.silver])
        LOGGER.exception("Silver MERGE failed")
        raise
    lineage.emit(RunState.COMPLETE, "silver_merge", run_id, [paths.bronze], [paths.silver])
    LOGGER.info("Silver MERGE completed", extra={"path": paths.silver})
