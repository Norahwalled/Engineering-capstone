# Data contracts

All incoming Kafka events will have versioned Pydantic contracts. Each version defines
field names, types, business invariants, and compatibility expectations. Contract
validation happens before data is allowed into the Bronze Delta table. Invalid payloads
are sent to the Kafka quarantine topic with the complete validation error and source
Kafka metadata.

The concrete event schema is intentionally introduced with the ingestion milestone,
where it can be exercised against an actual Kafka cluster and represented in the Silver
business-key strategy.
