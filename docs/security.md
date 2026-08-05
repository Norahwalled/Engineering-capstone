# Security

Secrets are supplied at runtime through a managed secret store and injected through the
execution environment. Credentials, keys, broker endpoints containing credentials, and
cloud account identifiers are never committed. Runtime identities use least-privilege
permissions for Kafka, object storage, vector database access, lineage publishing, and
observability.

All production network connections require TLS. Data storage uses encryption at rest.
Access, failures, and administrative changes must be auditable through centralized logs.
