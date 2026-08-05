# Synthetic sample data

## Purpose

`data/samples/` contains a deliberately small event set for local demonstrations and
automated contract tests. It exercises Kafka ingestion, quarantine routing, Bronze
delivery retention, historical Silver deduplication, the current-state Silver snapshot,
Gold daily aggregation, and RAG indexing without requiring a large download.

The files contain 18 records in total:

| File | Records | Purpose |
|---|---:|---|
| `valid_events.json` | 8 | Multiple customers, event types, dates, currencies, and amounts |
| `invalid_events.json` | 4 | Timezone, UUID, amount, currency, version, and extra-field failures |
| `late_events.json` | 3 | Older event times intentionally published after the normal batch |
| `duplicate_events.json` | 3 | Exact replays of IDs from the valid batch |

## Schema

Each file is a JSON array. A valid event follows the Pydantic `CustomerEvent` contract:

| Field | Meaning |
|---|---|
| `event_id` | Immutable UUID identifying one historical event |
| `event_type` | Business event category |
| `customer_id` | Synthetic customer identifier |
| `occurred_at` | Timezone-aware event timestamp, normalized to UTC during validation |
| `amount` | Non-negative decimal with at most two fractional digits |
| `currency` | Three-letter uppercase currency code |
| `document_text` | Optional synthetic support or transaction description |
| `schema_version` | Contract version; currently `1.0` |

Late arrival is represented by publication order: publish `valid_events.json` first and
`late_events.json` afterward. The latter has earlier `occurred_at` values, allowing Gold
to demonstrate event-time daily metrics when older facts arrive later.

## Safety and repository policy

All people, customers, events, amounts, and descriptions are fictional. The fixtures
contain no names, email addresses, phone numbers, account numbers, credentials, or source
system exports. They are safe to publish on GitHub and are not intended for production
analytics or model evaluation.

Only `data/README.md` and `data/samples/*.json` are tracked. Other content under `data/`
remains ignored so raw datasets, generated Delta tables, and checkpoints cannot be
committed accidentally.

## Contract-only testing

No services are required to validate the fixture files:

```bash
source .venv/bin/activate
python -m pytest tests/unit/test_sample_data.py
```

## Pipeline testing

Do not execute these commands until the local Docker Compose services are intentionally
started. Once Kafka and the validator are running, publish the fixtures from the host
through Kafka's exposed port:

```bash
export CAPSTONE_KAFKA_BOOTSTRAP_SERVERS=localhost:9094
python scripts/produce_sample_events.py data/samples/valid_events.json
python scripts/produce_sample_events.py data/samples/duplicate_events.json
python scripts/produce_sample_events.py data/samples/late_events.json
python scripts/produce_sample_events.py data/samples/invalid_events.json --raw
```

The invalid file uses `--raw` intentionally. Normal producer validation would reject
those records before Kafka, whereas raw publication verifies the real consumer-side
quarantine path.

After the validator processes the records, run the Airflow DAG using the existing
verification runbook. Expected behavior:

1. `raw.events` contains 18 deliveries.
2. `validated.events` contains the 14 contract-valid deliveries.
3. `quarantine.events` contains 4 rejected records with reasons and Kafka provenance.
4. Bronze retains all 14 valid deliveries, including 3 replayed IDs.
5. Historical Silver contains 11 unique events after deterministic `event_id` deduplication.
6. Current-state Silver contains the latest event per `(customer_id, event_type)`.
7. Gold counts all 11 unique historical events by customer and event day, including the
   three late-arriving facts on their original event dates.

For exact service commands and evidence collection, continue with
`docs/verification.md`. Reset Kafka offsets and Delta volumes between repeated full
demonstrations when a clean count is required.
