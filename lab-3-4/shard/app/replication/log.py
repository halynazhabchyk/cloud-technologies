import json
import logging
import os
import threading
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)

LogEntry = dict[str, Any]


class LeaderLog(Protocol):
    """Durable, ordered shard log the leader writes to (Kafka in the cluster)."""

    def append(self, operation: LogEntry) -> LogEntry:
        """Store one operation and return it with its 1-based ``offset``."""
        ...

    def read_all(self) -> list[LogEntry]:
        """Return every stored entry in offset order (leader recovery)."""
        ...


class LogGapError(RuntimeError):
    """An entry does not directly follow the last offset of the log."""


class ReplicationLog:

    def __init__(self, path: Path | None = None, keep_up_to: int | None = None):
        self._path = path
        self._entries: list[LogEntry] = []
        self._acks: dict[str, int] = {}
        self._changed = threading.Condition()

        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._entries = self._load(path)
            if keep_up_to is not None and self.last_offset > keep_up_to:
                self._entries = self._entries[:keep_up_to]
                self._rewrite()

    @staticmethod
    def _load(path: Path) -> list[LogEntry]:
        if not path.exists():
            return []

        entries: list[LogEntry] = []
        with path.open(encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    # A crash in the middle of a write leaves a partial last line.
                    logger.warning(
                        "Ignoring broken log line %s in %s", line_number, path
                    )
                    break
                if entry.get("offset") != len(entries) + 1:
                    logger.warning("Log %s is not contiguous at %s", path, line_number)
                    break
                entries.append(entry)
        return entries

    def _rewrite(self) -> None:
        assert self._path is not None
        tmp_path = self._path.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as file:
            for entry in self._entries:
                file.write(json.dumps(entry) + "\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(tmp_path, self._path)

    def _persist(self, entry: LogEntry) -> None:
        if self._path is None:
            return
        with self._path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(entry) + "\n")
            file.flush()
            os.fsync(file.fileno())

    @property
    def last_offset(self) -> int:
        return len(self._entries)

    def entries(self) -> list[LogEntry]:
        with self._changed:
            return list(self._entries)

    def entry(self, offset: int) -> LogEntry:
        """Return the entry at a 1-based log offset."""
        with self._changed:
            return self._entries[offset - 1]

    def append_replicated(self, entry: LogEntry) -> None:
        """Store an entry that already has its offset (from Kafka), keeping order."""
        with self._changed:
            if entry["offset"] != self.last_offset + 1:
                raise LogGapError(
                    f"Expected offset {self.last_offset + 1}, got {entry['offset']}"
                )
            self._persist(entry)
            self._entries.append(entry)
            self._changed.notify_all()

    def record_ack(self, replica_id: str, applied_offset: int) -> None:
        """Remember that a follower has durably applied the log up to an offset."""
        with self._changed:
            if applied_offset > self._acks.get(replica_id, -1):
                self._acks[replica_id] = applied_offset
                self._changed.notify_all()

    def acknowledged_by(self, offset: int) -> int:
        with self._changed:
            return sum(1 for acked in self._acks.values() if acked >= offset)

    def wait_for_acks(self, offset: int, followers: int, timeout: float) -> bool:
        """Wait until ``followers`` followers have applied ``offset``."""
        if followers <= 0:
            return True
        with self._changed:
            return self._changed.wait_for(
                lambda: (
                    sum(1 for acked in self._acks.values() if acked >= offset)
                    >= followers
                ),
                timeout=timeout,
            )

    def follower_progress(self) -> dict[str, int]:
        with self._changed:
            return dict(self._acks)


class OffsetStore:
    """Follower checkpoint: the last log offset applied and persisted locally."""

    def __init__(self, path: Path | None = None):
        self._path = path

    def load(self) -> int:
        if self._path is None or not self._path.exists():
            return 0
        try:
            return int(self._path.read_text(encoding="utf-8").strip() or 0)
        except ValueError:
            logger.warning("Ignoring unreadable offset file %s", self._path)
            return 0

    def save(self, offset: int) -> None:
        if self._path is None:
            return
        # Write to a temporary file and rename it: the checkpoint is never torn.
        tmp_path = self._path.with_suffix(".tmp")
        tmp_path.write_text(str(offset), encoding="utf-8")
        os.replace(tmp_path, self._path)
