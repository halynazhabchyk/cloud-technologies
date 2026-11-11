# Distributed Key-Value Store (milestones 3–4)

Distributed key-value store: a FastAPI coordinator routes records to shards by consistent hashing, and every shard is a replica set of three nodes, one leader and two followers, kept in sync through an append-only replication log in Apache Kafka (one single-partition topic per shard). The client API is specified in `openapi.yaml`.

## Requirements

- Python 3.10 or newer
- pip
- Docker with Docker Compose (for the cluster)

## Project layout

Coordinator (`coordinator/app`):

- `core/config.py` holds `CoordinatorSettings`, read from the environment; it validates the quorums.
- `api/routes` contains thin HTTP routes: `tables`, `records`, `shards`, `health`.
- `api/errors.py` maps domain errors to HTTP once: `InvalidRequestError` → 400, `ShardUnavailableError` → 503.
- `services/coordinator.py` is the business logic: tables, key validation, and record operations in single-node or sharded mode.
- `services/shard_client.py` is HTTP access to replicas: writes go to the leader with `acks=W`; reads go to `R` replicas and return the freshest answer.
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
- `storage/` contains the replica's in-memory record store with versions.

The coordinator's `main.py` exposes a `create_app(settings=None)` factory and the `app` object used by Uvicorn. The replica's `main.py` exposes only `create_app(settings=None, leader_log=None)`; Uvicorn calls it with `--factory`, so importing the module does not connect to Kafka. `loadtest/` contains the load-test script and the recorded results.

## Run the cluster

```sh
docker compose up --build
```

This starts Kafka, the coordinator (http://127.0.0.1:8000) and two shards with three replicas each:

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

To reproduce the three runs:

```sh
REPLICATION_FACTOR=1 WRITE_QUORUM=1 READ_QUORUM=1 docker compose up -d kafka coordinator shard1-a shard2-a
REPLICATION_FACTOR=3 WRITE_QUORUM=1 READ_QUORUM=1 docker compose up -d
docker compose up -d   # N=3, W=2, R=2
```

Run `docker compose down -v` between runs.

## Record API

Writing a record returns the selected `shardId` and its `version`. Reading `/tables/{tableName}/items/{partitionKey}`:

- with `?sortKey=...` returns one record (`shardId`, `replicaId`, `sortKey`, `value`, `version`) or `404`;
- without `sortKey` returns `shardId`, `replicaId` and all records in that partition (an empty list if there are none).

`shardId`, `replicaId` and `version` are `null` in single-node mode.

## Tests and lint

```sh
pip install -r requirements-dev.txt -r shard/requirements.txt
ruff check . && black --check .
python -m pytest -q
```

`tests/conftest.py` runs the coordinator with in-process replicas, so the sharded and replicated flows are tested without Docker:

- `test_replication.py` covers the log, the leader/follower flow, follower restart from its offset, and the write quorum;
- `test_replicated_flow.py` covers read spreading, stale reads vs quorum reads, and failover.

CI for this folder is `.github/workflows/ci.yml`. It also starts the full cluster with Docker Compose and checks that all 6 replicas register and that a write reaches every replica.

For a single-node local coordinator:

```sh
pip install -r coordinator/requirements.txt
uvicorn coordinator.app.main:app --reload
```

Set `SHARDING_ENABLED=false` for this mode. Tables and records are kept in the coordinator's memory, and the shard registration API is not exposed.

## Current limitations

- **Static leader, no failover.** If the leader is down, its shard cannot accept writes. Leader election is out of scope for this milestone.
- **No rollback of unconfirmed writes.** A write answered with `503 Write quorum not reached` is already in the leader's log and will still reach the followers. The client must treat it as "unknown", not "failed". Conflict resolution is the topic of milestone 4.
- **Liveness is detected lazily.** A replica is marked unavailable only after a request to it fails. There are no background health checks yet (milestone 4).
- **No log compaction.** The log grows forever, and a new follower replays it from offset 1.
- Table definitions live only in the coordinator's memory.
- Adding a shard changes routing but does not migrate records already stored on other shards.
