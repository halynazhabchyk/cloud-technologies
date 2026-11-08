"""Estimate routing changes when adding a shard; this does not move stored data."""

import argparse

from coordinator.app.sharding.consistent_hashing import ConsistentHashRing


def measure(keys_count: int) -> tuple[dict[str, int], dict[str, int], int]:
    before_ring = ConsistentHashRing()
    after_ring = ConsistentHashRing()
    for shard_id in ("shard-1", "shard-2"):
        before_ring.add_node(shard_id)
        after_ring.add_node(shard_id)
    after_ring.add_node("shard-3")

    before: dict[str, int] = {"shard-1": 0, "shard-2": 0, "shard-3": 0}
    after: dict[str, int] = {"shard-1": 0, "shard-2": 0, "shard-3": 0}
    moved = 0
    for key in (f"partition-{index}" for index in range(keys_count)):
        old_shard = before_ring.get_node(key)
        new_shard = after_ring.get_node(key)
        before[old_shard] += 1
        after[new_shard] += 1
        moved += old_shard != new_shard

    return before, after, moved


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure key routing changes after adding a third shard."
    )
    parser.add_argument(
        "--keys", type=int, default=10_000, help="Number of sample keys"
    )
    args = parser.parse_args()
    if args.keys < 1:
        parser.error("--keys must be at least 1")

    before, after, moved = measure(args.keys)
    percent = moved / args.keys * 100
    print(f"Sample keys: {args.keys}")
    print(f"Before (2 shards): {before['shard-1']} / {before['shard-2']}")
    print(
        "After (3 shards): "
        f"{after['shard-1']} / {after['shard-2']} / {after['shard-3']}"
    )
    print(f"Keys with changed shard assignment: {moved} ({percent:.2f}%)")
    print("This measures routing changes; it does not migrate stored records.")


if __name__ == "__main__":
    main()
