# Live verification evidence — 2026-08-05

> Historical record: this evidence predates the historical/current Silver refactor.
> Re-run `docs/verification.md` before treating its Silver and Gold results as evidence
> for the current implementation.

This evidence was captured from the running Docker Compose platform. No mocked Kafka,
Delta, OpenSearch, Airflow, Great Expectations, or OpenLineage service was used.

## Platform health

`docker compose ps --all` showed Kafka and OpenSearch as healthy and the Airflow
scheduler, webserver, validator, RAG API, Marquez, and PostgreSQL as running. The
one-shot Airflow, Kafka-topic, and storage initialization services exited with code 0.

## Kafka, Pydantic, and quarantine

The platform created the real topics `raw.events`, `validated.events`, and
`quarantine.events`. A valid fixture was accepted by the validator and appeared on
`validated.events`. A malformed message was sent directly to `raw.events` using the
Kafka console producer so that the consumer boundary was exercised.

The actual quarantine record included the original payload, Kafka provenance, and
Pydantic errors for all three invalid fields:

```json
{
  "source": {"topic": "raw.events", "partition": 1, "offset": 1},
  "rejection_reason": "occurred_at must include a timezone offset; amount >= 0; currency must match ^[A-Z]{3}$"
}
```

Validator log evidence: `validated event` and `quarantined invalid event`.

## Successful dependency-respecting Airflow run

The following command completed with the Airflow DagRun state `success`:

```bash
docker compose exec -T airflow-scheduler \
  airflow dags test capstone_modern_data_engineering 2026-08-05
```

The command executed, in dependency order:

1. `ensure_kafka_topics`
2. `bronze_ingestion`
3. `bronze_quality_gate`
4. `silver_merge`
5. `schema_enforcement`
6. `silver_quality_gate`
7. `gold_aggregate`
8. `gold_quality_gate`
9. `rag_indexing`

The final log reported `DagRun ... state=success`. The indexing task reported
`RAG indexing completed` and returned `1`.

## Delta Lake, MERGE, and schema enforcement

PySpark reads from the real Delta paths reported:

| Delta layer | Row count | Confirmed fields / result |
|---|---:|---|
| Bronze | 2 | event contract fields plus `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `ingested_at` |
| Silver | 1 | business entity `(customer-001, support_case)` with amount `125.50` |
| Gold | 1 | `event_day`, `event_count=1`, `total_amount=125.50`, `average_amount=125.500000` |

`DESCRIBE HISTORY` on Silver returned a real Delta operation `MERGE`; its metrics
included `numSourceRows=1`, `numTargetRowsMatchedUpdated=1`, and
`numTargetRowsInserted=0`.

The schema-enforcement task intentionally attempted to append
`unauthorized_column`. Delta raised `AnalysisException: A schema mismatch detected`
and described the extra field. The task succeeded because it correctly treated that
rejection as the expected enforcement outcome.

## Great Expectations success and controlled failure

All three successful quality tasks emitted `Quality gate passed`. The real
Great Expectations Spark expectations covered non-null keys, uniqueness, valid
non-negative amounts, and Gold aggregate bounds.

For the failure path, an exact duplicate valid event was sent to real Kafka. It passed
Pydantic validation and was written to Bronze. A separate DAG test run on
`2026-08-04` then failed at `bronze_quality_gate` with:

```text
QualityGateError: Great Expectations failed: event_id unique
```

Airflow listed `silver_merge`, `schema_enforcement`, `silver_quality_gate`,
`gold_aggregate`, `gold_quality_gate`, and `rag_indexing` as unrunnable. This proves
that the DAG dependency graph blocks downstream work after a quality failure.

## OpenLineage / Marquez

`GET /api/v1/namespaces/modern-data-engineering-ai/jobs` returned the jobs created by
the pipeline, including `bronze_ingestion`, `bronze_quality_gate`,
`silver_merge`, `schema_enforcement`, `gold_aggregate`, `gold_quality_gate`, and
`rag_indexing`. Job entries include named input and output datasets.

The backend returned `COMPLETED` states for successful runs and `FAILED` states for
the controlled Bronze quality failure. The application logs additionally show an
emitted START event before each task body.

The topic-initialization task was re-run in the final audit. It completed successfully,
emitted START and COMPLETE, and Marquez recorded `raw.events`, `validated.events`, and
`quarantine.events` as output datasets.

## OpenSearch, embeddings, hybrid search, RRF, and cross-encoder

`GET /knowledge-chunks/_mapping` returned a strict OpenSearch mapping with:

* `embedding` as `knn_vector` with dimension `384`;
* full-text `title` and `text` fields for BM25;
* citation fields `document_id`, `chunk_id`, `source_uri`, and `title`.

`GET /knowledge-chunks/_count` returned `1`. A live `HybridRetriever.retrieve()` call
loaded `sentence-transformers/all-MiniLM-L6-v2`, queried dense KNN and BM25 against
OpenSearch, fused the rankings with RRF, and loaded
`cross-encoder/ms-marco-MiniLM-L-6-v2` to produce a non-null `rerank_score`.

## Grounded answer and citations

The Compose platform runs the real local Ollama model `qwen2.5:0.5b` through its
OpenAI-compatible API; `ollama-init` completed with exit code 0 and `ollama list`
reported the downloaded 397 MB model. A live call to `POST /v1/answer` returned:

```json
{
  "answer": "Customer 001 reported that the August invoice is higher than the agreed service plan amount.",
  "citations": [{
    "document_id": "018fd263-0c9e-7c50-b902-d5dc7ac122fd",
    "chunk_id": "60b139e1be0b0047551385aff2877050fea34069218f5c92efa6d3f79a7439a5",
    "source_uri": "delta://silver/customer_events/018fd263-0c9e-7c50-b902-d5dc7ac122fd",
    "title": "support_case event for customer customer-001"
  }]
}
```

The answer is directly supported by the returned cited chunk. No external API key,
synthetic answer, or mocked model was used.
