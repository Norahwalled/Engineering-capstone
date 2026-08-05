"""Silver-to-Gold Delta job producing independent business aggregates."""

from __future__ import annotations

import logging

from openlineage.client.run import RunState
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, count, date_trunc
from pyspark.sql.functions import sum as spark_sum

from capstone_de.lakehouse.paths import LakehousePaths
from capstone_de.lakehouse.spark import create_spark_session
from capstone_de.lineage.emitter import LineageEmitter
from capstone_de.settings import Settings

LOGGER = logging.getLogger(__name__)


def aggregate_historical_events(history: DataFrame) -> DataFrame:
    """Aggregate history at a currency-safe customer, event-day, and currency grain."""
    return history.groupBy(
        "customer_id",
        date_trunc("day", "occurred_at").alias("event_day"),
        "currency",
    ).agg(
        count("event_id").alias("event_count"),
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount"),
    )


def build_gold_aggregates(settings: Settings, lineage: LineageEmitter) -> None:
    """Create currency-safe daily metrics from the complete historical Silver table."""
    paths = LakehousePaths.from_settings(settings)
    run_id = lineage.new_run_id()
    lineage.emit(RunState.START, "gold_aggregate", run_id, [paths.silver], [paths.gold])
    spark = create_spark_session(settings, "gold-aggregate")
    try:
        history = spark.read.format("delta").load(paths.silver)
        gold = aggregate_historical_events(history)
        gold.write.format("delta").mode("overwrite").option("overwriteSchema", "false").save(
            paths.gold
        )
    except Exception:
        lineage.emit(RunState.FAIL, "gold_aggregate", run_id, [paths.silver], [paths.gold])
        LOGGER.exception("Gold aggregation failed")
        raise
    lineage.emit(RunState.COMPLETE, "gold_aggregate", run_id, [paths.silver], [paths.gold])
    LOGGER.info("Gold aggregation completed", extra={"path": paths.gold})
