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
container with a PySpark Delta read. Bronze must retain Kafka metadata; Silver must have
the business key `(customer_id, event_type)`; Gold must contain daily `event_count`,
`total_amount`, and `average_amount` aggregates.

Publish a newer valid event with the same `(customer_id, event_type)` and rerun the DAG.
Verify that Silver updates the existing business-keyed entity through Delta `MERGE`
rather than duplicating it.

## 3. Schema enforcement and quality failure

The `schema_enforcement` Airflow task deliberately appends an unauthorized field to
Silver. It succeeds only when Delta rejects the write. Retain its task log.

For a quality-failure proof, insert a validly shaped duplicate business key directly
into Bronze with a controlled Spark command, trigger the DAG, and retain the failed
`silver_quality_gate` task result. The Gold and RAG indexing tasks must be upstream
failed or not executed.

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
