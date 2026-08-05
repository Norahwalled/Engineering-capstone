# Operational runbook

## Milestone 1 verification

1. Install project dependencies from `pyproject.toml`.
2. Copy `.env.example` to `.env` and set approved local values.
3. Run `make quality`.
4. Run `capstone validate-config`.

## Operational standards

Production changes require peer review, automated quality checks, and a successful
deployment validation. Incident response preserves source event metadata, relevant
Airflow run identifiers, and OpenLineage run identifiers so operators can trace a
failure without modifying source records.
