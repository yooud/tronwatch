# Changelog

Notable user-visible changes are recorded in this file. The project follows semantic versioning while it is pre-1.0: minor releases may contain configuration, storage, or event-schema changes that require migration.

## Unreleased

- Added bounded P2P catch-up from the persisted canonical tip after downtime.
- Added single-leader multi-peer catch-up failover, readiness gating, logs, and Prometheus metrics.

## 0.4.1 - 2026-09-26

Initial public release.

- Added multi-peer TRON P2P ingestion with duplicate suppression.
- Added dynamic inline, file, HTTP, and Redis watch sources.
- Added BoltDB persistence, fork tracking, exact-hash finality, and bounded retention.
- Added stdout, JSONL, webhook, and Redis Stream publishers with durable at-least-once delivery.
- Added health, readiness, Prometheus metrics, and container deployment files.
- Added automated tagged releases with cross-platform archives, checksums, and SBOM files.
