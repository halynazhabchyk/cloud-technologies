import hashlib
from bisect import bisect_left
from threading import RLock

DEFAULT_VIRTUAL_NODES_PER_SHARD = 100


class ConsistentHashRing:
    """Maps keys to node IDs using a SHA-1 ring with virtual nodes."""

    def __init__(
        self,
        virtual_nodes_per_node: int = DEFAULT_VIRTUAL_NODES_PER_SHARD,
    ):
        if virtual_nodes_per_node < 1:
            raise ValueError("At least one virtual node per node is required")

        self.virtual_nodes_per_node = virtual_nodes_per_node
        self._nodes: set[str] = set()
        self._points: list[tuple[int, str, int]] = []
        self._lock = RLock()

    @staticmethod
    def _hash(value: str) -> int:
        digest = hashlib.sha1(value.encode("utf-8")).digest()
        return int.from_bytes(digest, "big")

    def add_node(self, node_id: str) -> None:
        with self._lock:
            if node_id in self._nodes:
                return

            new_points = [
                (self._hash(f"{node_id}:{replica}"), node_id, replica)
                for replica in range(self.virtual_nodes_per_node)
            ]
            self._nodes.add(node_id)
            self._points = sorted([*self._points, *new_points])

    def get_node(self, key: str) -> str:
        with self._lock:
            if not self._points:
                raise LookupError("The consistent hash ring has no nodes")

            key_hash = self._hash(key)
            point_index = bisect_left(self._points, (key_hash, "", -1))
            if point_index == len(self._points):
                point_index = 0

            return self._points[point_index][1]

    @property
    def node_count(self) -> int:
        with self._lock:
            return len(self._nodes)
