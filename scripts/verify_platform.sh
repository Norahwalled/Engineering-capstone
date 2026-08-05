#!/usr/bin/env bash
set -euo pipefail

docker compose up --build -d
docker compose run --rm validator capstone ensure-topics
docker compose run --rm validator capstone produce /opt/capstone/examples/events/valid_customer_event.json
# Deliberately bypass the producer's Pydantic pre-validation to test the real
# consumer-side quarantine boundary with a malformed Kafka record.
docker compose exec -T kafka bash -c "printf '%s\\n' '{\"event_id\":\"018fd263-0c9e-7c50-b902-d5dc7ac122fe\",\"event_type\":\"support_case\",\"customer_id\":\"customer-002\",\"occurred_at\":\"2026-08-05T09:00:00\",\"amount\":\"-5.00\",\"currency\":\"saudi-riyal\",\"schema_version\":\"1.0\"}' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic raw.events"
docker compose exec airflow-scheduler airflow dags trigger capstone_modern_data_engineering
docker compose exec airflow-scheduler airflow dags test capstone_modern_data_engineering 2026-08-05
curl --retry 10 --retry-all-errors --retry-delay 2 -fsS \
  -X POST http://localhost:8081/v1/answer \
  -H 'content-type: application/json' \
  -d '{"question":"What did Customer 001 report about the August invoice?"}'
CAPSTONE_RUN_INTEGRATION=1 docker compose run --rm validator python -m pytest -m integration
