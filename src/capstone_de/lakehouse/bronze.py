"""Kafka-to-Bronze Structured Streaming job using real Delta Lake storage."""

from __future__ import annotations

import logging

from openlineage.client.run import RunState
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType

from capstone_de.lakehouse.paths import LakehousePaths
from capstone_de.lineage.emitter import LineageEmitter
from capstone_de.settings import Settings

LOGGER = logging.getLogger(__name__)

VALIDATED_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("occurred_at", TimestampType(), nullable=False),
        StructField("amount", DecimalType(16, 2), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("document_text", StringType(), nullable=True),
        StructField("schema_version", StringType(), nullable=False),
    ]
)


def read_validated_events(
    settings: Settings, spark_schema: StructType = VALIDATED_EVENT_SCHEMA
) -> DataFrame:
    """Read only post-Pydantic-validation Kafka events into a typed streaming DataFrame."""
    from capstone_de.lakehouse.spark import create_spark_session

    spark = create_spark_session(settings, "bronze-ingestion")
    kafka_frame = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_validated_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        kafka_frame.select(
            from_json(col("value").cast("string"), spark_schema).alias("event"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select("event.*", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp")
        .withColumn("ingested_at", current_timestamp())
    )


def start_bronze_stream(settings: Settings, lineage: LineageEmitter) -> StreamingQuery:
    """Write validated Kafka records to Bronze Delta and emit the stage lifecycle start."""
    paths = LakehousePaths.from_settings(settings)
    run_id = lineage.new_run_id()
    lineage.emit(
        state=RunState.START,
        job_name="bronze_ingestion",
        run_id=run_id,
        inputs=[settings.kafka_validated_topic],
        outputs=[paths.bronze],
    )
    try:
        query = (
            read_validated_events(settings)
            .writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{paths.checkpoints}/bronze")
            .trigger(availableNow=True)
            .start(paths.bronze)
        )
        query.awaitTermination()
    except Exception:
        lineage.emit(
            RunState.FAIL,
            "bronze_ingestion",
            run_id,
            [settings.kafka_validated_topic],
            [paths.bronze],
        )
        LOGGER.exception("Bronze streaming job failed")
        raise
    lineage.emit(
        RunState.COMPLETE,
        "bronze_ingestion",
        run_id,
        [settings.kafka_validated_topic],
        [paths.bronze],
    )
    LOGGER.info("Bronze streaming job completed", extra={"path": paths.bronze})
    return query
