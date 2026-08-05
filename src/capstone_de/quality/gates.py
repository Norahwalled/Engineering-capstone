"""Great Expectations validations for Bronze, Silver, and Gold Delta datasets."""

from __future__ import annotations

import logging
from typing import Protocol, cast

import great_expectations as gx
from openlineage.client.run import RunState
from pyspark.sql import DataFrame

from capstone_de.lakehouse.paths import LakehousePaths
from capstone_de.lakehouse.spark import create_spark_session
from capstone_de.lineage.emitter import LineageEmitter
from capstone_de.settings import Settings

LOGGER = logging.getLogger(__name__)


class QualityGateError(RuntimeError):
    """Raised when a Great Expectations validation must stop downstream work."""


class SparkExpectationBatch(Protocol):
    """Minimal Great Expectations batch interface required by this module."""

    def validate(self, expectation: object) -> object: ...


def _require_success(result: object, expectation: str) -> None:
    if getattr(result, "success", False) is not True:
        raise QualityGateError(f"Great Expectations failed: {expectation}; result={result}")


def _batch(frame: DataFrame) -> SparkExpectationBatch:
    """Create a Great Expectations Spark batch backed by the supplied real DataFrame."""
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_spark(
        name="quality_spark",
        force_reuse_spark_context=True,
        persist=False,
    )
    asset = source.add_dataframe_asset(name="quality_dataframe")
    batch_definition = asset.add_batch_definition_whole_dataframe(name="whole_dataframe")
    return cast(
        SparkExpectationBatch, batch_definition.get_batch(batch_parameters={"dataframe": frame})
    )


def validate_bronze(frame: DataFrame) -> None:
    """Enforce Bronze identity presence and provenance while allowing delivery replays."""
    dataset = _batch(frame)
    _require_success(
        dataset.validate(gx.expectations.ExpectColumnValuesToNotBeNull(column="event_id")),
        "event_id not null",
    )
    _require_success(
        dataset.validate(gx.expectations.ExpectColumnValuesToNotBeNull(column="kafka_offset")),
        "kafka_offset not null",
    )


def validate_silver(frame: DataFrame) -> None:
    """Enforce historical event identity and monetary constraints before Gold."""
    dataset = _batch(frame)
    _require_success(
        dataset.validate(gx.expectations.ExpectColumnValuesToNotBeNull(column="event_id")),
        "event_id not null",
    )
    _require_success(
        dataset.validate(gx.expectations.ExpectColumnValuesToBeUnique(column="event_id")),
        "event_id unique",
    )
    _require_success(
        dataset.validate(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")),
        "customer_id not null",
    )
    _require_success(
        dataset.validate(
            gx.expectations.ExpectColumnValuesToBeBetween(column="amount", min_value=0)
        ),
        "amount non-negative",
    )


def validate_silver_current(frame: DataFrame) -> None:
    """Enforce the one-row-per-customer-and-event-type current-state grain."""
    dataset = _batch(frame)
    _require_success(
        dataset.validate(gx.expectations.ExpectColumnValuesToNotBeNull(column="event_id")),
        "event_id not null",
    )
    _require_success(
        dataset.validate(
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["customer_id", "event_type"]
            )
        ),
        "current-state business key unique",
    )


def validate_gold(frame: DataFrame) -> None:
    """Enforce completeness and validity of Gold aggregate metrics."""
    dataset = _batch(frame)
    _require_success(
        dataset.validate(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")),
        "customer_id not null",
    )
    _require_success(
        dataset.validate(
            gx.expectations.ExpectColumnValuesToBeBetween(column="event_count", min_value=1)
        ),
        "event_count positive",
    )
    _require_success(
        dataset.validate(
            gx.expectations.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0)
        ),
        "total_amount non-negative",
    )


def run_quality_gate(layer: str, settings: Settings, lineage: LineageEmitter) -> None:
    """Validate a persisted Delta layer and raise on failure to gate the Airflow DAG."""
    paths = LakehousePaths.from_settings(settings)
    path_by_layer = {
        "bronze": paths.bronze,
        "silver": paths.silver,
        "silver_current": paths.silver_current,
        "gold": paths.gold,
    }
    validator_by_layer = {
        "bronze": validate_bronze,
        "silver": validate_silver,
        "silver_current": validate_silver_current,
        "gold": validate_gold,
    }
    if layer not in path_by_layer:
        raise ValueError(f"Unsupported quality layer: {layer}")
    run_id = lineage.new_run_id()
    job_name = f"{layer}_quality_gate"
    lineage.emit(RunState.START, job_name, run_id, [path_by_layer[layer]], [path_by_layer[layer]])
    try:
        frame = (
            create_spark_session(settings, job_name).read.format("delta").load(path_by_layer[layer])
        )
        validator_by_layer[layer](frame)
    except Exception:
        lineage.emit(
            RunState.FAIL, job_name, run_id, [path_by_layer[layer]], [path_by_layer[layer]]
        )
        LOGGER.exception("Quality gate failed", extra={"layer": layer})
        raise
    lineage.emit(
        RunState.COMPLETE, job_name, run_id, [path_by_layer[layer]], [path_by_layer[layer]]
    )
    LOGGER.info("Quality gate passed", extra={"layer": layer})
