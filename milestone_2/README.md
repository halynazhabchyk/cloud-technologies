# Distributed Key-Value Store — Milestone 2, Task 2

Distributed Key-Value Store using a FastAPI coordinator and in-memory storage shards. The client API is specified in `openapi.yaml`.

## Requirements

- Python 3.10 or newer
- pip

## Project layout

- `coordinator/app/api` contains HTTP routes, request schemas, and dependencies.
- `coordinator/app/core` contains coordinator configuration.
- `coordinator/app/services` contains coordination and shard request logic.
- `coordinator/app/sharding` contains the hash ring, shard registry, and the standalone resharding estimator.
- `coordinator/app/storage` contains the local storage contract and its in-memory implementation.
- `shard/app/api`, `shard/app/core`, and `shard/app/storage` contain the shard's HTTP API, configuration, and in-memory record store.
- Each service's `main.py` exposes a `create_app()` factory and the `app` object used by Uvicorn.

## Run the distributed service

Start the coordinator and two shards with Docker Compose:

```sh
docker compose up --build
```

The coordinator is available at http://127.0.0.1:8000. Each shard registers at startup. Record requests return `503` until at least two shards have registered.

The coordinator routes by `partitionKey`. Its SHA-1 consistent-hash ring has 100 virtual nodes per shard; `sortKey` identifies the record within the partition and does not affect shard placement.

To estimate how many partition keys get a new assignment when adding `shard-3`, run this standalone measurement from the project root:

```sh
python -m coordinator.app.sharding.measure_resharding --keys 10000
```

With the current ring and sample keys `partition-0` through `partition-9999`, the two-shard distribution is 5,106 / 4,894; after adding the third shard it is 3,377 / 2,963 / 3,660. Thus 3,660 keys (36.60%) change their routing assignment. This is an estimate based on the supplied key set and does not physically migrate stored records.

Writing a record returns the selected `shardId`. Reading `/tables/{tableName}/items/{partitionKey}` returns `shardId` and all records in that partition, including each record's `sortKey`; `shardId` is `null` in single-node mode. The shard's internal `GET /internal/records` endpoint lists every record stored on that shard.

For a single-node local coordinator, install the coordinator dependencies and run:

```sh
pip install -r coordinator/requirements.txt
uvicorn coordinator.app.main:app --reload
```

Set `SHARDING_ENABLED=false` for this local mode. Tables and records are stored and checked in the coordinator's in-memory storage. The shard registration API is not exposed in this mode, and data is lost when the coordinator restarts.

## Current limitations

- Adding a shard changes routing assignments but does not migrate records already stored on other shards.
- There is no replication or persistent storage.
- `openapi.yaml` is the client API specification. FastAPI also generates interactive documentation from the implemented routes.
