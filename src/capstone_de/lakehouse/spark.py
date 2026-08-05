"""Creation of a production-configured Spark session with Delta Lake extensions."""

from __future__ import annotations

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from capstone_de.settings import Settings


def create_spark_session(settings: Settings, application_name: str) -> SparkSession:
    """Create a Spark session configured for Delta Lake schema enforcement and ACID writes."""
    builder = (
        SparkSession.builder.appName(application_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", f"{settings.delta_base_path}/warehouse")
    )
    configured_builder = configure_spark_with_delta_pip(builder)
    configured_builder.config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    )
    return configured_builder.getOrCreate()
