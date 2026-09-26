# tronwatch

`tronwatch` is a lightweight outbound TRON P2P client for real-time transaction monitoring. It connects directly to one or more java-tron peers, filters pending and block transactions against a dynamic set of addresses and contracts, stores matching records in BoltDB, and publishes lifecycle events to configured destinations.

The service is intended for systems that need a small, selected part of the network stream without operating a FullNode solely for event ingestion. It does not provide RPC, TVM execution, historical backfill, or account state.

> **Project status:** `tronwatch` is pre-1.0 software. Validate it against your own peers, traffic, and failure scenarios before relying on it in production. Minor releases may include compatibility changes, which are recorded in [CHANGELOG.md](CHANGELOG.md).

## What it does

- receives pending transactions and blocks from multiple TRON peers;
- combines all peer observations into one serialized processing stream;
- filters top-level TRON contracts and recognized TRC-20 calls;
- reloads watched addresses and contracts from local or remote sources;
- tracks canonical inclusion, orphaning, reinclusion, and finalization;
- stores records and durable publisher queues in BoltDB;
- publishes to stdout, JSONL, HTTP webhooks, and Redis Streams;
- exposes Prometheus metrics, liveness, and readiness endpoints;
- limits retained finalized payloads without losing canonical block identifiers.

## Requirements

- Go 1.25.13 or newer when building from source;
- outbound TCP access to at least one compatible TRON P2P peer, normally on port `18888`;
- a writable local directory for BoltDB;
- at least one watch source containing addresses or contracts;
- an HTTPS Solidity endpoint if finalized events are required.

The network ID must match every configured peer. Common values are `11111` for mainnet and `201910292` for Nile. A practical starting allocation is 1 vCPU and 256 MiB RAM, but the required disk and memory depend on the match rate, publisher backlog, peer count, and retention policy.

## Quick start

Build and validate the example configuration:

```bash
make build
cp config.example.json config.json
cp watchlist.example.json watchlist.json
./bin/tronwatch check-config --config config.json
```

The example expects a mainnet java-tron peer at `127.0.0.1:18888`. Replace `p2p.peers` if the peer runs elsewhere, then start the service:

```bash
./bin/tronwatch run --config config.json
```

Application logs are written to stderr. Publisher output is independent and is written to its configured destination.

## Configuration overview

Configuration is strict JSON. Unknown fields, duplicate names, malformed addresses, invalid URLs, and non-positive intervals are rejected before the P2P client starts.

```json
{
  "p2p": {
    "peers": ["127.0.0.1:18888", "10.0.0.2:18888"],
    "network_id": 11111,
    "advertise_ip": "127.0.0.1"
  },
  "storage": {
    "path": "data/transactions.db",
    "finalized_retention_blocks": 256,
    "event_payload_mode": "compact_lifecycle"
  },
  "watch": {
    "refresh_interval": "1s",
    "sources": [
      {"name": "local", "type": "file", "path": "watchlist.json"}
    ]
  },
  "publishers": [
    {"name": "stdout", "type": "stdout"}
  ]
}
```

Watch sources return a complete snapshot with this shape:

```json
{
  "addresses": ["T..."],
  "contracts": ["41..."]
}
```

Addresses may use Base58Check or `41`-prefixed hexadecimal form. The effective watchlist is the union of the last valid snapshot from every source. A refresh failure keeps that source's last valid snapshot; a valid empty snapshot removes its contribution.

Supported watch sources:

| Type | Purpose |
|---|---|
| `inline` | Values stored directly in the configuration |
| `file` | A local JSON snapshot, suitable for atomic file replacement |
| `http` | An HTTPS control-plane endpoint |
| `redis_sets` | Separate Redis sets for addresses and contracts |

Supported publishers:

| Type | Delivery |
|---|---|
| `stdout` | One JSON event per line |
| `jsonl` | Local append-only files with optional rotation |
| `webhook` | HTTPS POST with `Idempotency-Key` |
| `redis_stream` | Redis Stream fields containing the event metadata and payload |

The publisher list may be empty. In that mode matching records remain in BoltDB and no outbox payload is created. Multiple publishers have independent durable queues, so a failed destination does not stop P2P ingestion or healthy destinations.

See [docs/configuration.md](docs/configuration.md) for all fields, defaults, security checks, and examples.

## Event lifecycle and delivery

A matching pending transaction is stored immediately. Block processing then produces these versioned lifecycle events:

- `included.v2` when the transaction enters the canonical branch;
- `orphaned.v2` when a reorganization removes its block;
- `reincluded.v2` when the transaction returns on the winning branch;
- `finalized.v2` when the canonical block matches the exact block ID reported by the configured Solidity endpoint.

Events are delivered at least once. A crash after external delivery but before the local acknowledgement can produce a duplicate, so consumers must deduplicate by `event_id`. Transaction state, lifecycle events, and publisher queue entries are committed atomically.

Finality is not inferred from block depth. Without a Solidity endpoint, transactions can become `included` but never `finalized`.

## Storage

BoltDB is the source of recovery for matched records, chain metadata, and outstanding deliveries. It uses an exclusive file lock, so offline commands must not open the database while the daemon is running.

`finalized_retention_blocks` controls how many finalized blocks keep their full local transaction payload. Older payloads are removed while a compact height-to-block-ID index remains available for fork checks. Set it to `0` only when unbounded local retention is intentional.

`event_payload_mode: "compact_lifecycle"` keeps the full `included` event and omits repeated raw transaction and peer data from later lifecycle events. Consumers using this mode must apply updates to the earlier included record.

Deleting BoltDB keys does not immediately shrink the file. Freed pages are reused for later writes.

## Operations

The observability listener provides:

- `/healthz` for process liveness;
- `/readyz` for peer, chain, finality, and outbox readiness;
- `/metrics` for Prometheus-format metrics.

The default listener is `127.0.0.1:9464`. A non-loopback address requires `observability.allow_public: true`; expose it only behind appropriate network controls.

Useful commands:

```bash
./bin/tronwatch version
./bin/tronwatch check-config --config config.json
./bin/tronwatch probe --url http://127.0.0.1:9464/readyz
./bin/tronwatch check-db --db data/transactions.db
./bin/tronwatch list --db data/transactions.db --limit 100
```

For a container deployment:

```bash
docker compose -f deploy/compose.yml up --build -d
```

Review `deploy/config.docker.example.json` before use. The example publishes metrics only on host loopback and stores the database in a named volume.

## Limits

- Multiple peers provide redundant observations, not quorum voting or full TRON state validation.
- The matcher handles top-level contract participants and recognized TRC-20 calldata. Internal TVM transfers and logs require receipt processing from another service.
- Historical P2P catch-up after downtime is not implemented. A detected parent gap makes readiness fail instead of silently declaring later data canonical.
- Remote watch snapshots are retained in memory but are not cached separately for cold startup.
- Webhook signing, a dead-letter queue, and an online query API are not included.

See [docs/architecture.md](docs/architecture.md) for processing invariants and failure behavior.

## Development

```bash
make verify
make race
```

`make verify` runs formatting checks, unit tests, `go vet`, and a clean binary build. The generated protobuf file in `internal/protocol` is committed so `protoc` is not required for a normal build.

## Contributing and security

Contributions are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow and commit conventions. Use the private process in [SECURITY.md](SECURITY.md) for suspected vulnerabilities and follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) in project spaces.

## License

Licensed under the [Apache License 2.0](LICENSE).
