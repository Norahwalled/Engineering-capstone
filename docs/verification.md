# Verification runbook

This project is not complete until each command below has been run against the Docker
Compose services and its evidence retained under `docs/evidence/`.

## 1. Real Kafka and Pydantic validation

Run `capstone ensure-topics`, publish both files in `examples/events/`, and inspect the
topics with `kafka-console-consumer.sh` inside the Kafka container. `raw.events` must
contain both messages; `validated.events` must contain only the valid event;
`quarantine.events` must contain the invalid payload, Kafka topic/partition/offset, and
Pydantic rejection JSON.

## 2. Bronze, Silver, and Gold Delta Lake

Run the Airflow DAG. Inspect the shared `lakehouse_data` volume from an application
container with a PySpark Delta read. Bronze must retain Kafka metadata. Historical
Silver must contain one row per unique `event_id`; current-state Silver must contain one
latest row per `(customer_id, event_type)`. Gold must contain daily `event_count`,
`total_amount`, and `average_amount` calculated from historical Silver.

Publish two events with different event IDs but the same `(customer_id, event_type)` and
rerun the DAG. Verify that historical Silver contains both events, current-state Silver
contains only the newer event, and Gold counts both on their respective event days.
Replay one event ID and verify that the historical Delta `MERGE` remains idempotent.

## 3. Schema enforcement and quality failure

The `schema_enforcement` Airflow task deliberately appends an unauthorized field to
Silver. It succeeds only when Delta rejects the write. Retain its task log.

Bronze deliberately permits replayed event IDs because it preserves source deliveries;
historical Silver performs deterministic deduplication. For a quality-failure proof,
insert a controlled duplicate `event_id` directly into historical Silver, run
`silver_history_quality_gate`, and retain the failed result. Gold and RAG indexing must
be upstream failed or not executed. Separately verify that the current-state gate
rejects duplicate `(customer_id, event_type)` rows.

## 4. OpenLineage

Inspect Marquez after one successful DAG run and one controlled failed run. For every
stage, retain event evidence showing START and COMPLETE on success; retain FAIL for the
controlled failure. Events must show their named input and output datasets.

## 5. RAG

After a successful index task, query OpenSearch to verify `embedding` vector fields and
BM25 text fields exist. Confirm that `ollama-init` completed successfully, then call
`POST /v1/answer` with a question supported by the indexed event document. The response
must include answer text and citations containing
`document_id`, `chunk_id`, `source_uri`, and `title`. Retain the response and matching
OpenSearch retrieval evidence.
