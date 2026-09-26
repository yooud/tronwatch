# Configuration reference

`tronwatch` reads strict JSON. Unknown fields, trailing JSON values, duplicate source or publisher names, malformed addresses, invalid URLs, and invalid durations cause startup to fail.

Durations use Go notation such as `250ms`, `15s`, `2m`, and `24h`.

## P2P connections

```json
{
  "p2p": {
    "peers": ["127.0.0.1:18888", "10.0.0.2:18888"],
    "network_id": 11111,
    "advertise_ip": "127.0.0.1",
    "node_id_file": "data/transactions.db.node-id",
    "catchup": {
      "enabled": true,
      "batch_size": 100,
      "request_timeout": "10s"
    }
  }
}
```

| Field | Meaning |
|---|---|
| `peers` | One or more `host:port` endpoints |
| `network_id` | TRON network identifier; `11111` for mainnet, `201910292` for Nile |
| `advertise_ip` | IP sent in the P2P hello message |
| `node_id_file` | Persistent 64-byte P2P identity; defaults to `<storage.path>.node-id` |
| `catchup.enabled` | Resume a persisted canonical chain after downtime; default `true` |
| `catchup.batch_size` | Blocks requested per fetch, from `1` through `100`; default `100` |
| `catchup.request_timeout` | Maximum wait for each chain inventory or block batch; default `10s` |

Every endpoint has an independent reconnect loop. All peers share one local node identity and feed one serialized ingestion stream. The identity file is created atomically with mode `0600` and must be retained with the database so restarts do not look like a new peer. Do not reuse one identity file concurrently across instances. The legacy singular field `peer` remains accepted but cannot be combined with `peers`.

java-tron may reject an immediate reconnect with transport code `3` (`RECENT_DISCONNECT`). TronWatch waits 65 seconds before retrying that peer, then resumes from the persisted tip. A persistent node identity does not bypass this peer-side cooldown.

When catch-up is needed, one peer becomes the sync leader and the other connections continue supplying pending transactions. A failed leader enters a cooldown and another eligible peer can continue from the last committed tip. Peers whose retained history starts after that tip are not selected. Readiness remains false until the persisted tip is explicitly confirmed at the remote head.

Fetches are capped at 100 blocks and paced below the java-tron synchronization request limit. Reducing `batch_size` lowers each burst but also reduces catch-up throughput.

A single advertised successor is fetched through the live inventory path. Historical catch-up starts from live inventory only when more than one block is missing, an unresolved parent exists, or another peer has already established a catch-up target.

Catch-up is resume-only: an empty database starts from live traffic instead of downloading the chain from genesis. Replayed blocks use the watchlist that is active at processing time, so this mechanism does not provide retrospective indexing for newly added targets.

## Storage

```json
{
  "storage": {
    "path": "data/transactions.db",
    "persist_all_peers": false,
    "sync_freelist": false,
    "finalized_retention_blocks": 256,
    "event_payload_mode": "compact_lifecycle"
  }
}
```

`path` is the BoltDB file. The parent directory is created when needed.

`persist_all_peers` controls peer provenance. The default `false` records the first pending peer and the first peer associated with each lifecycle transition. Same-state copies from additional peers are discarded before a writable BoltDB transaction. Set it to `true` only if a complete list of observing peers is required.

`sync_freelist: false` avoids writing the recoverable free-page index on every commit. Transaction and outbox durability still use BoltDB synchronization. Reopening a large database may take longer because the free-page index is rebuilt.

`finalized_retention_blocks` retains full payloads for that many finalized blocks. Older block and transaction payloads are removed, while a compact canonical height-to-block-ID index remains. Unacknowledged outbox data is retained independently. `0` disables pruning.

`event_payload_mode` accepts:

- `full`, which includes the complete record in every lifecycle event;
- `compact_lifecycle`, which keeps `included.v2` complete and removes repeated raw transaction and peer fields from later lifecycle events.

BoltDB reuses pages released by pruning but does not automatically reduce the physical file size.

## Watch sources

```json
{
  "watch": {
    "refresh_interval": "1s",
    "sources": [
      {
        "name": "local",
        "type": "file",
        "path": "watchlist.json"
      }
    ]
  }
}
```

Every source represents a complete snapshot:

```json
{
  "addresses": ["T..."],
  "contracts": ["41..."]
}
```

The active set is the union of the last valid snapshots. A successful empty snapshot removes that source's contribution. A read, decode, or validation error keeps its previous contribution.

Sources are required by default. Set `"required": false` when startup may proceed before a source has produced its first valid snapshot.

### Inline

```json
{
  "name": "fixed",
  "type": "inline",
  "addresses": ["41..."],
  "contracts": ["T..."]
}
```

At least one address or contract is required.

### File

```json
{
  "name": "local",
  "type": "file",
  "path": "watchlist.json"
}
```

Replace the file atomically when updating it so the reader never observes a partial write.

### HTTP

```json
{
  "name": "registry",
  "type": "http",
  "url": "https://control.example/watchlist",
  "token_env": "TRONWATCH_REGISTRY_TOKEN",
  "timeout": "2s",
  "required": false
}
```

The endpoint must return the complete JSON snapshot. HTTPS is required. Local development over plain HTTP requires `"allow_insecure_http": true`.

Use `url_env` instead of `url` when the endpoint itself contains credentials. `url` and `url_env` are mutually exclusive.

### Redis sets

```json
{
  "name": "redis-control",
  "type": "redis_sets",
  "url_env": "TRONWATCH_REDIS_URL",
  "addresses_key": "tronwatch:addresses",
  "contracts_key": "tronwatch:contracts",
  "timeout": "2s",
  "required": false
}
```

Each Redis member is one TRON address. At least one key must be configured. If both sets change together, update them through a control-plane strategy that exposes a consistent revision; two independently changing sets are not read as one Redis transaction.

## Publishers

`publishers` may contain any combination of `stdout`, `jsonl`, `webhook`, and `redis_stream`. It may also be empty, in which case events are not queued for external delivery.

Common delivery fields are optional:

```json
{
  "batch_size": 100,
  "flush_interval": "250ms",
  "max_delivery_latency": "250ms"
}
```

The dispatcher fetches at most `batch_size` events. A full backlog continues without an added delay. `max_delivery_latency` limits healthy queue polling, not network retry time. JSONL writes and acknowledges a complete batch; the other publisher types currently send events individually from the bounded fetch.

### Standard output

```json
{
  "name": "stdout",
  "type": "stdout"
}
```

Each event is emitted as one JSON line to stdout.

### JSONL files

```json
{
  "name": "archive",
  "type": "jsonl",
  "path": "data/events.jsonl",
  "max_bytes": 536870912,
  "max_files": 4,
  "batch_size": 500,
  "flush_interval": "1s",
  "max_delivery_latency": "1s"
}
```

`max_bytes` and `max_files` must both be positive or both be zero. Positive values enable rotation; zero values keep one unbounded file.

### HTTP webhook

```json
{
  "name": "payments",
  "type": "webhook",
  "url": "https://payments.example/tron-events",
  "token_env": "TRONWATCH_WEBHOOK_TOKEN",
  "timeout": "3s",
  "attempts": 3,
  "retry_backoff": "250ms"
}
```

The publisher sends one event per HTTPS POST and copies `event_id` into the `Idempotency-Key` header. A 2xx response acknowledges delivery. Network errors, 408, 429, and 5xx responses are retried. Other 4xx responses remain in the durable queue until the destination or configuration is corrected.

### Redis Stream

```json
{
  "name": "stream",
  "type": "redis_stream",
  "url_env": "TRONWATCH_REDIS_URL",
  "stream": "tronwatch:events",
  "timeout": "2s"
}
```

The stream entry contains `event_id`, `type`, and `payload`. Both `redis://` and `rediss://` URLs are accepted.

## Finality

```json
{
  "finality": {
    "url": "https://api.trongrid.io/walletsolidity/getnowblock",
    "token_env": "TRONGRID_API_KEY",
    "poll_interval": "15s",
    "timeout": "5s",
    "max_staleness": "2m",
    "required": true
  }
}
```

Finalization requires an exact match of both block height and block ID between the local canonical branch and the Solidity response. Omit the URL to disable finality. With `required: true`, a missing or stale checkpoint makes readiness fail. The endpoint must belong to the same network as the configured peers.

## Observability

```json
{
  "observability": {
    "listen": "127.0.0.1:9464",
    "block_stale_after": "30s",
    "minimum_peers": 1,
    "outbox_warn_depth": 10000
  }
}
```

The listener serves `/metrics`, `/healthz`, and `/readyz`. A non-loopback bind is rejected unless `allow_public` is explicitly set to `true`.

Readiness checks the configured peer threshold, active historical catch-up, recent block activity, unresolved chain gaps, required finality freshness, and publisher backlog.

## Runtime and logging

```json
{
  "runtime": {
    "max_duration": "0s",
    "minimum_free_bytes": 10737418240,
    "disk_check_interval": "30s"
  },
  "logging": {
    "json": true,
    "stats_interval": "30s"
  }
}
```

`max_duration: "0s"` means no time limit. A positive value is useful for bounded test runs. When `minimum_free_bytes` is positive, the process checks the filesystem at `disk_check_interval` and exits with an error before free space falls below the configured threshold.

`logging.json` selects structured JSON logs. `stats_interval` controls the periodic runtime summary.

## Validation

Environment variables referenced by `*_env` must be present during validation:

```bash
TRONWATCH_REDIS_URL='rediss://...' \
TRONWATCH_REGISTRY_TOKEN='...' \
./bin/tronwatch check-config --config config.json
```

Resolved secret values are not written to application logs.
