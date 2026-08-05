# Architecture

The production platform is organized into six runtime domains: Kafka ingestion,
Delta Lakehouse processing, data quality, RAG indexing and serving, orchestration, and
observability. Apache Airflow owns task ordering. A failed Great Expectations validation
prevents dependent Silver, Gold, and RAG tasks from running. OpenLineage publishes
START, COMPLETE, and FAIL events for each stage.

No local queue, pandas data lake, custom orchestrator, or in-memory vector index is a
substitute for the required production technologies. The local Compose stack runs real
Kafka, PostgreSQL, OpenSearch, Marquez, Airflow, and PySpark/Delta processes.

```text
Kafka -> Pydantic validation -> Bronze Delta -> Silver Delta MERGE -> Gold aggregates
                |                       |                 |
                v                       v                 v
           quarantine topic      Great Expectations    OpenSearch indexing
                                                           |
                                                   vector + BM25 retrieval
                                                           |
                                              RRF -> cross-encoder -> cited RAG answer
```
