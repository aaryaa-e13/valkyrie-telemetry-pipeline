"""Count rows of a Parquet folder from file footers only (no JVM, no data read).

Used by e2e.smoke_restart as an independent check on the data itself, next to
the status file written by our own listener.

What it counts: every `*.parquet` file under `root`, skipping hidden or
underscore paths (`_spark_metadata/`, `.part-*.crc`, `_temporary/`).

Caveat: Spark's file sink treats only files listed in `_spark_metadata` as
committed. A file left behind by an aborted batch would be counted here but
not by Spark. smoke_restart therefore cross-checks the footer total against
Spark's committed total (verify's `parquet_total_rows`); see D1.16.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq


@dataclass(frozen=True)
class ParquetCount:
    """Files and rows found under a folder."""

    files: int
    rows: int

    def as_dict(self) -> dict[str, int]:
        return {"files": self.files, "rows": self.rows}


def data_files(root: Path) -> list[Path]:
    """Parquet data files under root, excluding hidden/underscore paths."""
    if not root.exists():
        return []
    return sorted(
        p for p in root.rglob("*.parquet")
        if p.is_file()
        and not any(part.startswith(("_", ".")) for part in p.relative_to(root).parts)
    )


def count_rows(root: Path | str) -> ParquetCount:
    """Sum `num_rows` from each data file's footer.

    Raises:
        OSError / pyarrow.ArrowInvalid: a file is unreadable or not Parquet.
    """
    files = data_files(Path(root))
    return ParquetCount(len(files), sum(pq.read_metadata(f).num_rows for f in files))
