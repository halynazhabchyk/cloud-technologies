"""Table definitions (the catalog) persisted as a JSON file.

The shards rebuild their records from the replication log, but table
definitions live only on the coordinator. Without this file every coordinator
restart would forget the tables, and requests to existing data would get 404.
"""

import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

TableDefinitions = dict[str, dict[str, Any]]


def load_table_definitions(path: Path) -> TableDefinitions:
    """Read table definitions; a missing file means no tables yet."""
    if not path.exists():
        return {}
    tables: TableDefinitions = json.loads(path.read_text(encoding="utf-8"))
    logger.info("Loaded %s table definition(s) from %s", len(tables), path)
    return tables


def save_table_definitions(path: Path, tables: TableDefinitions) -> None:
    """Write table definitions atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # DEFENSE: write to a temporary file, then rename. os.replace is atomic, so
    # a crash mid-write leaves the old complete file, never a half-written one.
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(tables, indent=2), encoding="utf-8")
    os.replace(tmp_path, path)
