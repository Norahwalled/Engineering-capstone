# Rubric traceability

| Rubric requirement | Implementation | Required execution evidence |
|---|---|---|
| Kafka producer and consumer | `ingestion/producer.py`, `ingestion/validator.py` | Topic records from `raw.events` and `validated.events` |
| Pydantic ingestion validation | `domain/events.py`, `ingestion/validator.py` | Valid event accepted; invalid event rejected |
| Quarantine and reason | `application/contracts.py`, `ingestion/validator.py` | `quarantine.events` payload with rejection JSON |
| Bronze/Silver/Gold Delta | `lakehouse/bronze.py`, `silver.py`, `gold.py` | Delta table reads and Airflow task logs |
| Business-keyed MERGE | `lakehouse/silver.py` | Updated event changes existing `(customer_id, event_type)` row |
| Schema enforcement | `lakehouse/schema_enforcement.py` | Rejected unauthorized-column write log |
| RAG retrieval and citations | `rag/` | OpenSearch index, API response, and cited source chunks |
| Airflow dependency gating | `airflow/dags/capstone_pipeline.py` | DAG graph and failed gate with downstream halt |
| Great Expectations gates | `quality/gates.py` | Validation success and controlled failed checkpoint |
| OpenLineage lifecycle events | `lineage/emitter.py` | Marquez START, COMPLETE, and FAIL events per stage |

Use [verification.md](verification.md) to collect the evidence. Declared dependencies,
source code, and screenshots alone are not accepted as proof; retain command output and
real service results.
