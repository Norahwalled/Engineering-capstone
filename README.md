# Modern Data Engineering for AI Systems

Production-oriented, event-driven data engineering platform for customer-support
intelligence. It validates operational events at an Apache Kafka boundary, builds a
Delta Lake Bronze/Silver/Gold lakehouse using PySpark, enforces quality gates through
Great Expectations, publishes OpenLineage events, and provides grounded RAG answers
from real OpenSearch vector and BM25 search with Reciprocal Rank Fusion and
cross-encoder reranking.

## Required technologies

The implementation uses Apache Kafka through `confluent-kafka`, Pydantic, PySpark,
Delta Lake, Apache Airflow, Great Expectations, OpenLineage, OpenSearch as the real
vector database, embedding models, hybrid search, Reciprocal Rank Fusion, and a
cross-encoder reranker. No queue replacement, pandas/Parquet lakehouse, custom
orchestrator, or in-memory vector index is used.

## Architecture

```text
Kafka raw.events
  -> Pydantic validation consumer
       -> validated.events -> Spark -> Bronze Delta -> GE gate -> Silver Delta MERGE
       -> quarantine.events                               -> GE gate -> Gold Delta aggregate
                                                                       -> GE gate -> chunk/embed
                                                                                     -> OpenSearch

Question -> dense vector search + BM25 -> Reciprocal Rank Fusion -> cross-encoder
         -> grounded LLM answer + source/chunk citations

Airflow coordinates bounded jobs; OpenLineage emits START, COMPLETE, and FAIL for each.
```

## Repository layout

```text
src/capstone_de/domain/          Pydantic business contracts
src/capstone_de/ingestion/       Kafka producer, consumer, and quarantine routing
src/capstone_de/lakehouse/       PySpark/Delta Bronze, Silver, Gold, MERGE, schema proof
src/capstone_de/quality/         Great Expectations quality gates
src/capstone_de/lineage/         OpenLineage lifecycle emitter
src/capstone_de/rag/             Chunking, embeddings, OpenSearch, hybrid RAG API
airflow/dags/                    Production orchestration DAG
infrastructure/                  Database bootstrap and Docker deployment assets
tests/                           Unit and real-service integration checks
docs/                            Architecture, operating, security, and rubric evidence guides
```

## Prerequisites

- Docker Engine with Docker Compose v2
- At least 8 GB memory allocated to Docker
- Python 3.11 for local development
- At least 12 GB memory allocated to Docker when running the local Ollama model

## Configure

Copy the non-secret application defaults and create unique local development secrets:

```bash
cp .env.example .env
printf '\nAIRFLOW_DATABASE_PASSWORD=%s\n' "$(openssl rand -base64 32)" >> .env
printf 'AIRFLOW_FERNET_KEY=%s\n' "$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')" >> .env
printf 'AIRFLOW_ADMIN_USERNAME=%s\n' "capstone-admin" >> .env
printf 'AIRFLOW_ADMIN_EMAIL=%s\n' "admin@example.local" >> .env
printf 'AIRFLOW_ADMIN_PASSWORD=%s\n' "$(openssl rand -base64 32)" >> .env
```

The Compose stack runs a real local Ollama model (`qwen2.5:0.5b`) behind its
OpenAI-compatible API. No external API key is required for the default deployment.
Set `CAPSTONE_LLM_BASE_URL`, `CAPSTONE_LLM_MODEL`, and, when applicable,
`CAPSTONE_LLM_API_KEY` only when replacing the local service with an authorized remote
OpenAI-compatible endpoint.

## Run the full platform

```bash
docker compose up --build -d
docker compose run --rm validator capstone ensure-topics
docker compose run --rm validator capstone produce /opt/capstone/examples/events/valid_customer_event.json
docker compose run --rm validator capstone produce /opt/capstone/examples/events/invalid_customer_event.json
docker compose exec airflow-scheduler airflow dags test capstone_modern_data_engineering 2026-08-05
curl -X POST http://localhost:8081/v1/answer \
  -H 'content-type: application/json' \
  -d '{"question":"What did Customer 001 report about the August invoice?"}'
```

Open Airflow at `http://localhost:8080`, Marquez/OpenLineage at
`http://localhost:5000`, OpenSearch at `http://localhost:9200`, Ollama at
`http://localhost:11434`, and the RAG API at `http://localhost:8081`.

## Verify required success and failure paths

Use the repeatable runbook in [docs/verification.md](docs/verification.md). It requires
proof of valid ingestion, quarantine routing, Delta schema enforcement rejection,
quality-gate blocking, lifecycle lineage events, and cited RAG answers. The convenience
script is executed with:

```bash
bash scripts/verify_platform.sh
```

## Local development quality checks

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
make quality
```

## Security and operations

Read [docs/security.md](docs/security.md) before any shared or production deployment,
and [docs/operational-runbook.md](docs/operational-runbook.md) for run, failure, replay,
and recovery procedures. The Compose deployment is a real local integration environment;
production deployment requires TLS, managed secrets, TLS-enabled Kafka, authenticated
OpenSearch, and managed persistent storage.

## Training program attribution

Completed under **SDAIA Academy — Modern Data Engineering for AI Systems**, 5-day
capstone cohort, 1–5 August 2026. Program resources:
https://github.com/SDAIAAcademy

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE).
