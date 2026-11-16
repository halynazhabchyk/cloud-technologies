# Distributed Key-Value Store (milestones 3–4)

Distributed key-value store: a FastAPI coordinator routes records to shards by consistent hashing, and every shard is a replica set of three nodes, one leader and two followers, kept in sync through an append-only replication log in Apache Kafka (one single-partition topic per shard). The client API is specified in `openapi.yaml`.

Milestone 3 added replication (tag `m3`). Milestone 4 adds observability (Prometheus metrics, JSON logs with trace IDs, a Grafana dashboard, alerts), background health checks, Last-Write-Wins versioning, an MCP server for AI agents, `/version`, and background rebalancing when a shard joins. The Ukrainian defense guide is [`docs/milestone-4.md`](docs/milestone-4.md); design decisions are in [`docs/adr/`](docs/adr/).

## Requirements

- Python 3.10 or newer
- pip
- Docker with Docker Compose (for the cluster)

## Project layout

Coordinator (`coordinator/app`):

- `core/config.py` holds `CoordinatorSettings`, read from the environment; it validates the quorums.
- `api/routes` contains thin HTTP routes: `tables`, `records`, `shards`, `health`, `cluster` (Prometheus discovery, rebalancing).
- `api/errors.py` maps domain errors to HTTP once: `InvalidRequestError` → 400, `WriteConflictError` → 409, `ShardUnavailableError` → 503.
- `services/coordinator.py` is the business logic: tables, key validation, LWW timestamps, and record operations in single-node or sharded mode.
- `services/shard_client.py` is HTTP access to replicas: writes go to the leader with `acks=W`; reads go to `R` replicas and return the freshest answer. Every call carries the trace ID and is timed.
- `services/health_checker.py` polls every replica's `/health` in the background.
- `services/rebalancer.py` moves records to a shard that joined the ring.
- `services/metrics.py` holds the coordinator's metrics (replica state, call latency, moved keys).
- `sharding/` contains the consistent-hash ring, the shard/replica registry, and the resharding estimator.
- `storage/` contains the single-node storage contract and its in-memory implementation.

Replica (`shard/app`):

- `core/config.py` holds `ReplicaSettings` (role, leader URL, Kafka, data directory).
- `api/routes` has `records` (internal CRUD; writes only on the leader) and `replication` (`/internal/ack`).
- `registration.py` registers the replica with the coordinator and re-registers it when needed.
- `replication/node.py` is `ReplicaNode`, the leader/follower state machine over the log and the store.
- `replication/kafka_log.py` is the leader's Kafka log: topic creation, the producer, reading the topic back.
- `replication/log.py` holds the `LeaderLog` protocol, the follower's local copy of the log, the acknowledgement tracking, and the follower's offset checkpoint.
- `replication/follower.py` is the follower's Kafka consumer loop.
- `storage/` contains the replica's in-memory record store; `storage/versioning.py` is the Last-Write-Wins rule.
- `metrics.py` holds the replica's gauges (keys, applied offset, replication lag).

Shared and other folders:

- `common/` is copied into both images: trace IDs (`trace.py`), JSON logs (`json_logging.py`), request metrics and the `/metrics` + `/version` routes (`metrics.py`, `http_observability.py`), the error format (`errors.py`), build info and the clock.
- `mcp_server/` is the MCP server for AI agents (stdio).
- `monitoring/` has the Prometheus config and alert rules, the Alertmanager config, and the provisioned Grafana data source and dashboard.

The coordinator's `main.py` exposes a `create_app(settings=None)` factory and the `app` object used by Uvicorn. The replica's `main.py` exposes only `create_app(settings=None, leader_log=None)`; Uvicorn calls it with `--factory`, so importing the module does not connect to Kafka. `loadtest/` contains the load-test script and the recorded results.

## Run the cluster

```sh
GIT_SHA=$(git rev-parse --short HEAD) docker compose up -d --build
```

This starts Kafka, the coordinator (http://127.0.0.1:8000), two shards with three replicas each, and the monitoring stack: Prometheus (http://localhost:9090), Alertmanager (http://localhost:9093) and Grafana (http://localhost:3000, dashboard "KV store: cluster overview", login `admin` / `admin` or anonymous view). In PowerShell set the build argument with `$env:GIT_SHA = git rev-parse --short HEAD` first.

| Shard | Leader | Followers |
|-------|--------|-----------|
| `shard-1` | `shard1-a` (:8011) | `shard1-b` (:8012), `shard1-c` (:8013) |
| `shard-2` | `shard2-a` (:8021) | `shard2-b` (:8022), `shard2-c` (:8023) |

- `GET /health` on the coordinator shows how many shards and replicas are registered, plus N, W, R and whether R + W > N.
- `GET /shards` lists every shard with its replicas, their roles and availability.
- `GET /health` on a replica shows its role and applied offset. On the leader it also shows each follower's acknowledged offset and lag; on a follower, its lag behind the leader.
- `GET /internal/records` on a replica lists what that replica stores, including record versions.

Every replica registers itself with the coordinator at startup, giving its `shardId`, `replicaId`, `role` and URL. Every `REGISTRATION_CHECK_SECONDS` (default 10) it checks `GET /shards` and registers again if the coordinator has forgotten it, so `docker compose restart coordinator` recovers on its own. Record requests return `503` until at least two shards have a registered leader.

## Sharding

The coordinator routes by `partitionKey`. Its SHA-1 consistent-hash ring has 100 virtual nodes per shard. `sortKey` identifies the record within the partition and does not affect placement. The ring places *shards*, not replicas: all three replicas of a shard hold the same keys.

To estimate how many partition keys get a new assignment when adding `shard-3`:

```sh
python -m coordinator.app.sharding.measure_resharding --keys 10000
```

| Method | 2 shards | 3 shards | Keys moved |
|--------|----------|----------|------------|
| Consistent hashing, 100 vnodes | 5,106 / 4,894 | 3,377 / 2,963 / 3,660 | 3,660 (36.60%) |
| `hash % N` | 4,956 / 5,044 | 3,369 / 3,286 / 3,345 | 6,589 (65.89%) |

The theoretical minimum is 1/3 of the keys (33.33%). The script measures routing changes only and does not migrate stored records.

## Replication

### The log: Apache Kafka

Every shard has its own Kafka topic, `kv-log-shard-1` and `kv-log-shard-2`, with **one partition**. Kafka keeps the messages of a partition in strict order and numbers them with contiguous offsets, so the topic is the shard's ordered write log. Each message is one operation, for example `{"op": "put", "table": "orders", "partitionKey": "c1", "sortKey": "o1", "value": {...}}`. The leader creates the topic at startup with `retention.ms=-1`: the log is never deleted, because recovery replays it. Kafka runs as a single broker in KRaft mode (no ZooKeeper) in `docker-compose.yml`.

The log offset used everywhere is `Kafka offset + 1`, so `0` means "nothing applied yet". This offset is also the record's **version**.

### Writes: leader produces to Kafka

1. The coordinator sends `PUT`/`DELETE` to the **leader** of the owning shard, with `acks=W`.
2. Under one lock, the leader produces the operation to the shard topic and waits for Kafka's confirmation. The producer uses `acks=all` and `enable.idempotence=true`, so retries create neither duplicates nor reordering. Only then does the leader apply the operation to its in-memory store, so the order in Kafka equals the apply order.
3. The leader keeps an in-memory mirror of the entries for acknowledgements and `/health`. After a restart it rebuilds its store by reading the topic from the beginning.
4. The leader answers once `W` replicas, itself included, have applied the entry. If they do not confirm within `ACK_TIMEOUT_SECONDS`, it returns `503 Write quorum not reached`. The entry stays in Kafka and still reaches the followers; there is no rollback (see limitations).
5. Followers reject writes with `409`.

### Followers: consume the topic

- Every follower has a Kafka consumer, with its own `group.id` equal to its replica id, assigned to partition 0 of its shard's topic.
- Entries are applied strictly in offset order. Entries at or below the applied offset are skipped, so re-delivery is idempotent. A gap (an offset that skips one) is rejected.
- After each batch the follower reports its applied offset to the leader with `POST /internal/ack`. The leader counts these acknowledgements for the write quorum.

### Follower offset and restart (extended)

- The follower **does not commit offsets to Kafka** (`enable.auto.commit=false`). It keeps its own checkpoint next to its data, so data and offset can never disagree.
- It appends consumed entries to its own `DATA_DIR/log.jsonl`, applies them, and then saves the applied offset to `DATA_DIR/offset`. The save is atomic: write a temp file, then rename it.
- On restart, the follower drops local entries after the saved offset and rebuilds its store from its local log. It then `seek`s the consumer to that offset in Kafka, so it reads only the messages it missed. The logs show `Replica shard-2-b recovered up to offset 2` followed by `Follower shard-2-b consumes kv-log-shard-2 from log offset 3`.

### Kafka is required

`KAFKA_BOOTSTRAP_SERVERS` is mandatory: without it a replica fails at startup. The unit tests need no broker. They pass the leader an in-memory `FakeTopic` (`tests/fake_kafka.py`) through `create_app(settings, leader_log)`, and followers read the same topic through `FakeConsumer`, so the real leader and follower code runs.

### Reads: spread over replicas, quorum (extended)

- The coordinator orders the replicas of the owning shard round-robin, so consecutive reads go to different replicas.
- It asks `R` replicas. Every replica answers with the header `X-Applied-Offset`, and the coordinator returns the answer of the replica with the highest offset.
- This works because all replicas apply one log in one order: the replica with the highest offset has seen every write the others have seen.
- A replica that fails a request (connection error or 5xx) is skipped for `REPLICA_COOLDOWN_SECONDS` and the next one is asked, so reads fail over to live replicas.
- If fewer than `R` replicas answer, the read returns `503 Read quorum not reached`.
- Read responses include `replicaId`, which shows the spreading and the failover.

| Setting (coordinator env) | Default in compose | Meaning |
|---------------------------|--------------------|---------|
| `REPLICATION_FACTOR` | 3 | N, replicas per shard |
| `WRITE_QUORUM` | 2 | W, replicas that must hold a write before it is confirmed |
| `READ_QUORUM` | 2 | R, replicas asked on a read |

With W = 2 and R = 2, R + W = 4 > N = 3. Any 2 replicas that are read share at least one replica with any 2 that confirmed the last write, so a read always sees it. With W = 1 and R = 1, writes and reads are faster, but a read can hit a follower that has not caught up yet (stale read). `tests/test_replicated_flow.py` demonstrates both cases. To override the quorums:

```sh
WRITE_QUORUM=1 READ_QUORUM=1 docker compose up -d
```

### Failure scenarios checked on the running cluster

| Scenario | Result |
|----------|--------|
| One follower of a shard stopped | Writes succeed (W = 2: leader + the other follower); reads skip the dead replica |
| Both followers stopped | `PUT` → `503 Write quorum not reached`; reads → `503 Read quorum not reached` |
| Stopped follower started again | Recovers its store from its log up to its saved offset, fetches only the missing entries, lag 0 |
| Coordinator restarted | Replicas re-register within ~10 s |
| Leader stopped | Writes to that shard → `503` (no leader election in this milestone); reads still served by followers when R allows |

## Observability (milestone 4)

### Trace ID and JSON logs

- Every request gets a trace ID: the `X-Trace-Id` header of the client if it is valid (1–64 letters, digits, `.`, `_`, `-`), otherwise a new UUID. It is returned in the `X-Trace-Id` response header and in every error body.
- The coordinator passes it to every replica it calls; the leader stores it in the log entry (`traceId`), so followers log the same ID when they apply the write.
- All services log JSON lines to stdout with `ts`, `level`, `service`, `replica`, `trace_id`, `msg` and request fields (`method`, `path`, `status`, `duration_ms`). `LOG_LEVEL` (default `INFO`) sets the level.

```sh
curl -X PUT 'localhost:8000/tables/orders/items/c1?sortKey=o1' -H 'X-Trace-Id: demo-trace-001' \
  -H 'Content-Type: application/json' -d '{"value":{"total":10}}'
docker compose logs --no-log-prefix | grep demo-trace-001
```

The output shows the coordinator routing the write, the leader serving it and all three replicas applying offset N with the same trace ID.

### Metrics

Every service exposes `/metrics` in the Prometheus format. Prometheus scrapes the coordinator directly and discovers replicas from `GET /internal/prometheus-targets` on the coordinator (HTTP service discovery), so a shard that joins later is scraped automatically.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `kv_request_duration_seconds` | histogram | service, method, route, status | Request latency; p50/p95/p99 with `histogram_quantile` |
| `kv_requests_total` | counter | service, method, route, status | Throughput (`rate`) and error rate (`status=~"5.."`) |
| `kv_shard_call_duration_seconds` | histogram | shard, replica, method, outcome | Coordinator → replica calls |
| `kv_replica_up` | gauge | shard, replica, role | 1/0 from the coordinator's health checks |
| `kv_replication_lag_seconds` | gauge | shard, follower | Leader's view: age of the oldest entry the follower has not acknowledged |
| `kv_replication_lag_entries` | gauge | shard, follower | Entries the follower has not acknowledged |
| `kv_keys_per_shard` | gauge | shard, replica, role | Live records on the replica |
| `kv_rebalance_keys_moved_total` | counter | — | Records moved by rebalancing |
| `kv_build_info` | gauge | service, version, git_sha | Build of the running service |

`route` is the route pattern (`/tables/{tableName}/items/{partitionKey}`), not the real path, so the number of series does not grow with the number of keys. `/metrics`, `/health`, `/internal/ack` and the discovery endpoint are neither measured nor logged.

### Dashboard

Grafana loads the data source and the dashboard from `monitoring/grafana` at startup. Rows: **Cluster state** (healthy / down replicas, firing alerts, shards on the ring, moved keys, a UP/DOWN timeline per replica), **Requests** (throughput, coordinator and shard p50/p95/p99, 5xx error rate, coordinator → replica p99), **Replication and data** (lag in seconds and entries, keys per shard, keys per replica).

### Health checks and failure detection

The coordinator calls `/health` of every replica every `HEALTH_CHECK_SECONDS` (5 s) with a `HEALTH_CHECK_TIMEOUT` (1 s). A failed check marks the replica unhealthy (reads skip it, `kv_replica_up` = 0, a `Replica ... is down` warning in the log) until a check succeeds again (`... is back up`). Detection takes at most interval + timeout ≈ 6 s; a failed client request still marks a replica for `REPLICA_COOLDOWN_SECONDS` immediately. Followers re-send their offset to the leader at least every 5 s, so the leader's lag view stays current even after the leader restarts.

`GET /health` on the coordinator always answers 200 while the process works (Docker's healthcheck and replica startup depend on it); `ready` says whether two shards have a leader, `status` is `degraded` when a replica is down. `GET /version` on every service returns the version and the git commit (`GIT_SHA` build argument).

### Alerts

`monitoring/prometheus/alerts.yml`, evaluated every 5 s; firing alerts are shown in Prometheus (`/alerts`) and Alertmanager (http://localhost:9093).

| Alert | Condition |
|-------|-----------|
| `ReplicaDown` | `kv_replica_up == 0` for 10 s |
| `CoordinatorDown` | `up{job="coordinator"} == 0` for 10 s |
| `HighP99Latency` | p99 of a service > 500 ms for 30 s |
| `ReplicationLagHigh` | `kv_replication_lag_seconds > 10` for 10 s |

Checked on the running cluster:

| Scenario | Result |
|----------|--------|
| `docker compose stop shard1-b` | Health checker: down after ~4 s; `ReplicaDown` firing after ~19 s |
| Follower stopped + writes to its shard for 30 s | `ReplicationLagHigh` firing (lag 45 s, 33 entries) |
| Follower started again | Catches up, lag 0, both alerts resolved |
| `docker compose pause shard1-a` (hung leader) + writes | Writes to shard-1 time out after 3 s → coordinator p99 ≈ 4.9 s → `HighP99Latency` and `ReplicaDown` firing |

## Last-Write-Wins (milestone 4)

- The coordinator stamps every write with a timestamp in microseconds (`timestamp` in the response). A client may set it with `X-Write-Timestamp` to imitate a second coordinator whose clock differs.
- A record's version is `(timestamp, offset)`: the bigger timestamp wins; with equal timestamps the later log entry (bigger offset) wins (`shard/app/storage/versioning.py`).
- The leader checks the rule before appending to the log and answers `409` when the stored version is newer. Every replica applies the same rule when it applies an entry, so all replicas decide the same.
- A delete leaves a tombstone with its timestamp: a delayed older write of the same key loses instead of resurrecting it.

```sh
curl -X PUT 'localhost:8000/tables/orders/items/lww?sortKey=o1' -H 'X-Write-Timestamp: 2000' -H 'Content-Type: application/json' -d '{"value":{"v":"new"}}'   # 201
curl -X PUT 'localhost:8000/tables/orders/items/lww?sortKey=o1' -H 'X-Write-Timestamp: 1000' -H 'Content-Type: application/json' -d '{"value":{"v":"old"}}'   # 409
```

## MCP server (milestone 4)

`mcp_server/` exposes the store to AI agents over the Model Context Protocol (official Python SDK, stdio transport). It is a client of the coordinator's public API.

| Tool | Operation |
|------|-----------|
| `kv_exists` | EXISTS (`HEAD`) |
| `kv_create` | CREATE / replace (`PUT`) |
| `kv_read` | READ one record or a partition (`GET`) |
| `kv_delete` | DELETE (`DELETE`); marked destructive; removed when `MCP_ALLOW_DELETE=false` |
| `list_tables`, `cluster_health` | read-only helpers |

Table creation, shard registration and rebalancing are deliberately not exposed. Every tool call uses a trace ID `mcp-<uuid>`, returned to the agent and visible in the logs.

Run it from this folder (the client starts the process):

```sh
pip install -r mcp_server/requirements.txt
python -m mcp_server.server                     # COORDINATOR_URL defaults to http://127.0.0.1:8000
npx @modelcontextprotocol/inspector python -m mcp_server.server   # browser UI for manual checks
```

To use it from Claude Code, add a project `.mcp.json` (see `docs/milestone-4.md`).

## Rebalancing (milestone 4, extended)

When a new shard's leader registers on a ring that already has two shards, the coordinator waits `REBALANCE_DELAY_SECONDS` (5 s) and moves, in a background thread, every record whose owner changed: copy to the new owner with the original timestamp, then delete from the old owner with the same timestamp. LWW makes both steps safe against concurrent client writes and makes a repeated run harmless. Until it finishes, a read that misses on the new owner is retried on the previous owner. `GET /rebalance` shows the progress; `POST /rebalance/{shardId}` repeats it.

```sh
docker compose --profile rebalance up -d   # starts shard3-a/b/c (:8031-8033)
curl localhost:8000/rebalance
```

Checked on a clean cluster with 300 records (300 partition keys): 114 moved to shard-3 (38%; the hash-ring estimate for 10,000 keys is 36.6%), shard-1/2/3 ended with 90/96/114 records, all 300 keys were readable afterwards, and Prometheus discovered the three new replicas (10 targets).

## Load test (extended)

`loadtest/loadtest.py` is a closed-loop load generator: N threads send point reads and writes back to back for a fixed time, then it reports throughput and p50/p95/p99 latency.

```sh
python loadtest/loadtest.py --duration 20 --concurrency 16 --read-ratio 0.5 --label "..." --json loadtest/results.jsonl
```

The table shows results for 20 s, 16 threads, 50% reads and 1,000 keys, with Docker Desktop on one Windows laptop. Raw data is in `loadtest/results.jsonl`. The first row was measured when the replica still had a file-log mode without Kafka; that mode has been removed, so a new baseline (N=1 with Kafka) must be measured with the command below.

| Configuration | Throughput, ops/s | Read p50 / p95 / p99, ms | Write p50 / p95 / p99, ms |
|---------------|-------------------|--------------------------|---------------------------|
| No replication, no Kafka (1 replica per shard) | 220.5 | 62.5 / 90.0 / 115.0 | 69.1 / 99.6 / 123.5 |
| Kafka replication, async (N=3, W=1, R=1) | 196.3 | 70.2 / 106.1 / 129.9 | 75.6 / 110.2 / 147.1 |
| Kafka replication, quorum (N=3, W=2, R=2) | 183.5 | 80.2 / 111.7 / 131.3 | 78.1 / 110.7 / 133.4 |

- **Async replication with Kafka** costs about 11% of throughput. Every write is now also produced to Kafka with `acks=all`, and Kafka, 6 replicas instead of 2, and the followers' consumers share the same CPU.
- **The quorum** costs about 17% of throughput and adds about 10–18 ms to p50:
  - a write waits until at least one follower has consumed it from Kafka, applied it and sent `POST /internal/ack`;
  - a read asks two replicas one after another.
- p99 of the async and quorum runs is within the noise of a single laptop run (147 vs 133 ms for writes), so only throughput and p50 are compared.
- An early version of the Kafka follower used `consume(500, timeout=1s)`. It waited for a full batch or the whole timeout, so every write reached the followers about 1 s late and quorum writes crawled. The follower now waits only for the first message with `poll()` and takes the rest without waiting.
- The absolute latencies are high mostly because of the test setup, not the store: Docker Desktop port forwarding on Windows, plus a new HTTP connection per coordinator→replica request. So compare the configurations, not the absolute numbers.

### Cost of milestone 4

Same command, same laptop, measured on the same day one after another (the machine was slower than on the day of the table above, so only this pair is compared):

| Version | Throughput, ops/s | Read p50 / p99, ms | Write p50 / p99, ms |
|---------|-------------------|--------------------|---------------------|
| Milestone 3 (tag `m3`), N=3, W=2, R=2, two runs | 137.8 / 132.9 | 104.8 / 178.9 | 102.2 / 184.7 |
| Milestone 4, full stack with Prometheus/Grafana | 113.3 | 121.7 / 223.8 | 125.8 / 237.2 |
| Milestone 4, without monitoring containers, two runs | 116.0 / 113.2 | 118.7 / 222.0 | 124.6 / 246.2 |
| Milestone 4, `LOG_LEVEL=WARNING` | 114.9 | 119.2 / 223.2 | 128.1 / 247.4 |

Milestone 4 costs about 16% of throughput on this CPU-bound laptop setup (Kafka alone used ~2 cores). The monitoring containers (≈2%), the request logs and the type of HTTP middleware (a plain ASGI middleware replaced `@app.middleware`, no change) were ruled out; the remaining per-request work (metrics, trace propagation, LWW checks, health checks) was not profiled further, so the exact split is unknown.

To reproduce the three runs:

```sh
REPLICATION_FACTOR=1 WRITE_QUORUM=1 READ_QUORUM=1 docker compose up -d kafka coordinator shard1-a shard2-a
REPLICATION_FACTOR=3 WRITE_QUORUM=1 READ_QUORUM=1 docker compose up -d
docker compose up -d   # N=3, W=2, R=2
```

Run `docker compose down -v` between runs.

## Record API

Writing a record returns the selected `shardId`, its `version` (log offset) and `timestamp` (LWW); a write that loses Last-Write-Wins returns `409`. Reading `/tables/{tableName}/items/{partitionKey}`:

- with `?sortKey=...` returns one record (`shardId`, `replicaId`, `sortKey`, `value`, `version`, `timestamp`) or `404`;
- without `sortKey` returns `shardId`, `replicaId` and all records in that partition (an empty list if there are none).

`shardId`, `replicaId`, `version` and `timestamp` are `null` in single-node mode. Errors have the form `{"error": "...", "trace_id": "..."}`.

## Tests and lint

```sh
pip install -r requirements-dev.txt -r shard/requirements.txt
ruff check . && black --check .
mypy --ignore-missing-imports --explicit-package-bases common coordinator mcp_server
(cd shard && MYPYPATH=.. mypy --ignore-missing-imports app)
python -m pytest -q
```

`tests/conftest.py` runs the coordinator with in-process replicas, so the sharded and replicated flows are tested without Docker:

- `test_replication.py` covers the log, the leader/follower flow, follower restart from its offset, and the write quorum;
- `test_replicated_flow.py` covers read spreading, stale reads vs quorum reads, and failover;
- `test_observability.py` covers trace IDs, the error format, JSON logs, metrics and `/version`;
- `test_last_write_wins.py` covers the LWW rule, conflicts, tombstones and follower convergence;
- `test_cluster_operations.py` covers health checks, Prometheus discovery and rebalancing;
- `test_mcp_server.py` calls the MCP tools through a real MCP client session.

CI for this folder is `.github/workflows/ci.yml`. It also starts the full cluster with Docker Compose and checks that all 6 replicas register, that a write reaches every replica, and that `/version`, `/metrics` and Prometheus discovery work.

For a single-node local coordinator:

```sh
pip install -r coordinator/requirements.txt
uvicorn coordinator.app.main:app --reload
```

Set `SHARDING_ENABLED=false` for this mode. Tables and records are kept in the coordinator's memory, and the shard registration API is not exposed.

## Current limitations

- **Static leader, no failover.** If the leader is down, its shard cannot accept writes. Leader election is out of scope for this milestone.
- **No rollback of unconfirmed writes.** A write answered with `503 Write quorum not reached` is already in the leader's log and will still reach the followers. The client must treat it as "unknown", not "failed".
- **Last-Write-Wins trusts clocks.** A write stamped by a clock that is behind silently loses to an older write; a timestamp far in the future blocks every later write of that key. Concurrent updates are not merged: one of them is lost.
- **Tombstones are never removed**, and the log is never compacted: both grow forever.
- **Rebalancing is one pass.** A record written to the old owner between the scan and the delete (only possible for writes in flight while the ring changes) stays there; shards can only be added, not removed.
- **No alert notifications.** Alertmanager has no receiver integration (Telegram, Slack, e-mail); alerts are visible in its UI.
- Table definitions live only in the coordinator's memory.
