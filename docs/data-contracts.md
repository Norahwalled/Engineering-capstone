# Data contracts

All incoming Kafka events will have versioned Pydantic contracts. Each version defines
field names, types, business invariants, and compatibility expectations. Contract
validation happens before data is allowed into the Bronze Delta table. Invalid payloads
are sent to the Kafka quarantine topic with the complete validation error and source
Kafka metadata.

Historical Silver has one row per immutable `event_id` and preserves every distinct
validated business event. Replayed deliveries of the same event ID are resolved using
ingestion time and Kafka provenance. The optional current-state Silver table is derived
from that history and has one latest row per `(customer_id, event_type)`. Analytical
Gold products must use historical Silver so current-state selection cannot erase facts.
