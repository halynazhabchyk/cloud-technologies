"""Estimate routing changes when adding a shard; this does not move stored data.

Compares the consistent-hash ring used by the coordinator with plain
``hash(key) % N`` on the same sample keys.
"""

import argparse
import hashlib

from coordinator.app.sharding.consistent_hashing import ConsistentHashRing

SHARDS_BEFORE = ("shard-1", "shard-2")
SHARDS_AFTER = ("shard-1", "shard-2", "shard-3")

Result = tuple[dict[str, int], dict[str, int], int]


def _sample_keys(keys_count: int) -> list[str]:
    return [f"partition-{index}" for index in range(keys_count)]


def _modulo_shard(key: str, shards: tuple[str, ...]) -> str:
    digest = hashlib.sha1(key.encode("utf-8")).digest()
    return shards[int.from_bytes(digest, "big") % len(shards)]


def measure(keys_count: int) -> Result:
    """Consistent hashing with virtual nodes: 2 shards -> 3 shards."""
    before_ring = ConsistentHashRing()
    after_ring = ConsistentHashRing()
    for shard_id in SHARDS_BEFORE:
        before_ring.add_node(shard_id)
        after_ring.add_node(shard_id)
    after_ring.add_node("shard-3")

    return _compare(keys_count, before_ring.get_node, after_ring.get_node)


def measure_modulo(keys_count: int) -> Result:
    """Plain SHA-1(key) % N: 2 shards -> 3 shards."""
    return _compare(
        keys_count,
        lambda key: _modulo_shard(key, SHARDS_BEFORE),
        lambda key: _modulo_shard(key, SHARDS_AFTER),
    )


def _compare(keys_count, route_before, route_after) -> Result:
    before = dict.fromkeys(SHARDS_AFTER, 0)
    after = dict.fromkeys(SHARDS_AFTER, 0)
    moved = 0
    for key in _sample_keys(keys_count):
        old_shard = route_before(key)
        new_shard = route_after(key)
        before[old_shard] += 1
        after[new_shard] += 1
        moved += old_shard != new_shard

    return before, after, moved


def _print_result(title: str, keys_count: int, result: Result) -> None:
    before, after, moved = result
    percent = moved / keys_count * 100
    print(title)
    print(f"  Before (2 shards): {before['shard-1']} / {before['shard-2']}")
    print(
        "  After (3 shards):  "
        f"{after['shard-1']} / {after['shard-2']} / {after['shard-3']}"
    )
    print(f"  Keys with changed shard assignment: {moved} ({percent:.2f}%)")


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

    print(f"Sample keys: {args.keys}")
    _print_result(
        "Consistent hashing (100 virtual nodes per shard):",
        args.keys,
        measure(args.keys),
    )
    _print_result("hash % N:", args.keys, measure_modulo(args.keys))
    print("Theoretical minimum when adding a 3rd shard: 33.33% (1/3 of keys)")
    print("This measures routing changes; it does not migrate stored records.")


if __name__ == "__main__":
    main()
