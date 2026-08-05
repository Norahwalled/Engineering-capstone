"""Executable proof that Delta Lake rejects unauthorized schema evolution."""

from __future__ import annotations

import logging

from openlineage.client.run import RunState
from pyspark.sql.functions import lit

from capstone_de.lakehouse.paths import LakehousePaths
from capstone_de.lakehouse.spark import create_spark_session
from capstone_de.lineage.emitter import LineageEmitter
from capstone_de.settings import Settings

LOGGER = logging.getLogger(__name__)


class SchemaEnforcementError(RuntimeError):
    """Raised if a deliberately incompatible write is incorrectly accepted."""


def verify_schema_enforcement(settings: Settings, lineage: LineageEmitter) -> None:
    """Attempt an incompatible append and pass only when Delta Lake rejects it."""
    paths = LakehousePaths.from_settings(settings)
    run_id = lineage.new_run_id()
    lineage.emit(
        RunState.START, "silver_schema_enforcement", run_id, [paths.silver], [paths.silver]
    )
    spark = create_spark_session(settings, "schema-enforcement")
    try:
        incompatible = (
            spark.read.format("delta")
            .load(paths.silver)
            .limit(1)
            .withColumn("unauthorized_column", lit("blocked"))
        )
        try:
            incompatible.write.format("delta").mode("append").option("mergeSchema", "false").save(
                paths.silver
            )
        except Exception as error:
            LOGGER.info(
                "Delta schema enforcement correctly rejected incompatible write", exc_info=error
            )
        else:
            raise SchemaEnforcementError("Delta Lake accepted an unauthorized schema change")
    except Exception:
        lineage.emit(
            RunState.FAIL, "silver_schema_enforcement", run_id, [paths.silver], [paths.silver]
        )
        LOGGER.exception("Schema enforcement verification failed")
        raise
    lineage.emit(
        RunState.COMPLETE, "silver_schema_enforcement", run_id, [paths.silver], [paths.silver]
    )
