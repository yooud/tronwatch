# Architecture

```text
TRON peers -> live/catch-up coordinator -> serialized ingestion -> matcher -> BoltDB -> publisher workers
                                                              ^          |
                                                              |          +-> lifecycle and outbox
                inline / file / HTTP / Redis -> watch union
Solidity endpoint -> exact solid block ID -----------------> finalizer
Runtime state ---------------------------------------------> health and metrics
```

## Processing model

Each peer has an independent connection and reconnect loop. Decoded transactions and whole blocks enter a single ingestion path. This preserves deterministic state changes while still accepting the first useful observation from any peer.

After a restart, a non-empty database builds a sparse locator from its finalized checkpoint through its canonical tip. One eligible peer becomes the catch-up leader, returns the common block and a contiguous inventory, and serves bounded fetch batches. Every block is committed through the normal serialized ingestion path before the next batch advances. The other peers still deliver pending transactions, but live block inventories are held until catch-up explicitly confirms the committed tip at the network head. If the leader fails, another peer resumes from the durable tip after a bounded cooldown.

An empty database does not initiate catch-up from genesis. Historical blocks are filtered with the watchlist active when they are replayed; changing the watchlist does not trigger an older range scan.

The watch manager treats every source as a complete snapshot. It validates a new snapshot before atomically replacing that source's contribution to the union. A temporary refresh error leaves the previous valid contribution active.

The matcher extracts participants from supported top-level TRON contracts. For `TriggerSmartContract`, it also recognizes the participant arguments of common TRC-20 transfer and approval selectors. Matching does not execute TVM bytecode or inspect receipts.

## Durable state

BoltDB contains:

- matched transaction records;
- block parent links and canonical branch metadata;
- the finalized checkpoint and compact canonical index;
- immutable event payloads;
- independent acknowledgement state for each publisher.

A transaction change, its lifecycle event, and the corresponding outbox references are written in one BoltDB transaction. With one publisher the queue uses an inline fast path. With multiple publishers, one payload is shared by short per-publisher references and is removed only after the last acknowledgement.

Delivery is at least once. External delivery can succeed immediately before a crash prevents the local acknowledgement; the retry then carries the same `event_id`.

## Chain rules

Blocks are linked by parent ID rather than arrival order. A connected branch must become strictly longer before it replaces the canonical branch, preventing equal-height oscillation.

The service emits:

1. `included.v2` for a matching transaction on the canonical branch;
2. `orphaned.v2` if a reorganization removes that block;
3. `reincluded.v2` if the transaction appears on the new canonical branch;
4. `finalized.v2` after the local block height and exact block ID match the Solidity checkpoint.

A reorganization below the finalized checkpoint is rejected. An unknown parent is retained as a bounded unresolved candidate and makes readiness fail; the block is not declared canonical without a connected ancestry.

## Failure behavior

| Failure | Behavior |
|---|---|
| Watch source refresh fails | Keep its last valid snapshot and report the error |
| One peer disconnects | Reconnect it independently while other peers continue |
| Catch-up leader disconnects or times out | Release leadership, retain the committed tip, and let another eligible peer resume |
| Peer cannot serve the persisted tip | Keep catch-up pending and wait for a peer with sufficient retained history |
| Catch-up response is malformed or non-contiguous | Reject the session without applying unverified progress |
| Duplicate peer observation arrives | Skip same-state durable writes unless full peer provenance is enabled |
| Publisher fails | Keep its outbox item and retry without blocking ingestion or other publishers |
| Process stops during a write | Recover the last committed BoltDB state |
| Process stops after external delivery | Retry with the same `event_id` |
| Solidity endpoint is stale or unavailable | Stop advancing finality; readiness depends on `finality.required` |
| Disk free space crosses the configured guard | Exit with an error before exhausting the filesystem |

## Retention

When `finalized_retention_blocks` is positive, full finalized block and transaction payloads older than the configured window are deleted after durable events have been queued. The compact canonical block ID remains. Outstanding publisher payloads are retained independently until acknowledged.

BoltDB reuses freed pages but does not shrink its file automatically. Physical compaction is a separate maintenance operation.

## Trust boundaries

Peer fan-in and catch-up failover provide redundancy, not consensus validation. The service validates P2P framing, block IDs, ancestry, and response continuity, but it does not execute TRON state transitions. It trusts the configured Solidity endpoint for finality. Use multiple well-operated peers, protect remote watch and publisher endpoints with TLS, and keep the observability listener private unless access controls are provided externally.
