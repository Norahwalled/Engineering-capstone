# Architecture

The production platform is organized into six runtime domains: Kafka ingestion,
Delta Lakehouse processing, data quality, RAG indexing and serving, orchestration, and
observability. Apache Airflow owns task ordering. A failed Great Expectations validation
prevents dependent Silver, Gold, and RAG tasks from running. Historical Silver retains
one row per immutable `event_id`; a separate current-state Silver snapshot retains the
latest row per `(customer_id, event_type)`. Gold and RAG read historical Silver.
OpenLineage publishes
START, COMPLETE, and FAIL events for each stage.

No local queue, pandas data lake, custom orchestrator, or in-memory vector index is a
substitute for the required production technologies. The local Compose stack runs real
Kafka, PostgreSQL, OpenSearch, Marquez, Airflow, and PySpark/Delta processes.

```text
Kafka -> Pydantic validation -> Bronze Delta -> historical Silver MERGE
                |                       |                 |       |
                v                       v                 |       v
           quarantine topic      Great Expectations      |   current-state Silver
                                                        v
                                          Gold customer-day-currency aggregates
                                                        |
                                                OpenSearch indexing
                                                        |
                                               vector + BM25 retrieval
                                                        |
                                           RRF -> cross-encoder -> cited answer
```
