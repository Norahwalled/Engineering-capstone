"""OpenLineage lifecycle event emitter used by every executable pipeline stage."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import uuid4

from openlineage.client import OpenLineageClient
from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState

from capstone_de.settings import Settings

LOGGER = logging.getLogger(__name__)


class LineageEmitter:
    """Publishes standard START, COMPLETE, and FAIL events to an OpenLineage backend."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = OpenLineageClient(url=settings.openlineage_url)

    def emit(
        self,
        state: RunState,
        job_name: str,
        run_id: str,
        inputs: list[str],
        outputs: list[str],
    ) -> None:
        """Emit a lifecycle event with all known input and output datasets."""
        event = RunEvent(
            eventType=state,
            eventTime=datetime.now(UTC).isoformat(),
            run=Run(runId=run_id),
            job=Job(namespace=self._settings.openlineage_namespace, name=job_name),
            inputs=[
                Dataset(namespace=self._settings.openlineage_namespace, name=name)
                for name in inputs
            ],
            outputs=[
                Dataset(namespace=self._settings.openlineage_namespace, name=name)
                for name in outputs
            ],
            producer=f"{self._settings.service_name}/0.1.0",
        )
        self._client.emit(event)
        LOGGER.info(
            "emitted lineage event", extra={"job": job_name, "run_id": run_id, "state": state.value}
        )

    def new_run_id(self) -> str:
        """Generate a unique run identifier shared by all lifecycle events for a stage."""
        return str(uuid4())
