# Strict rubric audit — 2026-08-05

> Historical audit: the Silver/Gold model has since been refactored. Its live pipeline
> claims must be re-verified against the current revision.

| Requirement | Implemented? | Evidence | Missing items | Risk level | Potential score impact |
|---|---|---|---|---|---|
| Kafka producer and consumer | Yes | Live Kafka topics and validator logs | None | Low | None |
| Pydantic schema validation | Yes | Valid record forwarded; malformed record quarantined with field errors | None | Low | None |
| Dead-letter/quarantine reason | Yes | Real `quarantine.events` payload contains original payload, offset, and rejection reason | None | Low | None |
| Bronze Delta layer | Yes | Successful Kafka Structured Streaming task and Delta read with Kafka metadata | None | Low | None |
| Silver Delta layer and business-keyed MERGE | Yes | `DESCRIBE HISTORY` reports `MERGE` and matched-row update metrics | None | Low | None |
| Gold aggregate layer | Yes | Delta read contains event count, total amount, and average amount | None | Low | None |
| Delta schema enforcement | Yes | Unauthorized-field append raised real Delta `AnalysisException` | None | Low | None |
| Airflow orchestration and dependency gating | Yes | Full successful DagRun and controlled failed quality run with downstream tasks unrunnable | None | Low | None |
| Great Expectations gates | Yes | Bronze, Silver, and Gold gates passed; duplicate-Bronze failure was blocked | None | Low | None |
| OpenLineage lifecycle | Yes | Marquez jobs include datasets and `COMPLETED`/`FAILED` live run states; logs show START emission | None | Low | None |
| Real vector database | Yes | Live OpenSearch `knn_vector` mapping and indexed document count | None | Low | None |
| Embeddings | Yes | `all-MiniLM-L6-v2` loaded during live indexing/retrieval | None | Low | None |
| Hybrid dense + BM25 search | Yes | Live KNN and lexical search executed by `HybridRetriever` | None | Low | None |
| Reciprocal Rank Fusion | Yes | Live retrieval returned non-null `fused_score` | None | Low | None |
| Cross-encoder reranking | Yes | Live retrieval returned non-null `rerank_score` | None | Low | None |
| Cited grounded answer | Yes | Live Ollama `qwen2.5:0.5b` response contains a grounded answer plus document, chunk, URI, and title citations | None | Low | None |
| Documentation and reproducible Compose startup | Yes, pending fresh-restart validation | Compose bootstrap now includes `storage-init`; runbook and evidence updated | Fresh-volume re-run after the final Dockerfile edit has not yet been recorded | Medium | Small operational reproducibility risk |

All scored pipeline requirements have been proven with live services. The final cited
answer uses the real local Ollama model rather than an external hosted API.
