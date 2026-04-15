#!/usr/bin/env python3
import argparse
import csv
import glob
import hashlib
import os
import sqlite3
import sys
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def _strip_bom(s: str) -> str:
    # Some CSVs may include UTF-8 BOM at the start of the first header cell.
    return s.lstrip("\ufeff")


def _sha256_for_values(values: Sequence[str]) -> str:
    h = hashlib.sha256()
    sep = b"\x1f"  # unlikely delimiter
    for v in values:
        if v is None:
            v = ""
        if not isinstance(v, str):
            v = str(v)
        b = v.encode("utf-8", errors="replace")
        h.update(b)
        h.update(sep)
    return h.hexdigest()


def _read_header(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return []
        return [_strip_bom(h) for h in header]


def _iter_rows(path: str) -> Iterable[Dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # DictReader can return None for missing columns depending on python version.
            yield {k: ("" if v is None else v) for k, v in row.items()}


def merge_and_dedup(
    *,
    input_dir: str,
    pattern: str,
    output_file: str,
    sqlite_file: str,
) -> Tuple[int, int]:
    files = sorted(glob.glob(os.path.join(input_dir, pattern)))
    if not files:
        raise SystemExit(f"No files found for pattern: {os.path.join(input_dir, pattern)}")

    # First pass: build a unified column list (union of all CSV headers).
    columns: List[str] = []
    seen_cols = set()
    for p in files:
        header = _read_header(p)
        for col in header:
            if col not in seen_cols:
                seen_cols.add(col)
                columns.append(col)

    if not columns:
        raise SystemExit("No columns found across input CSVs.")

    # Prepare disk-backed set for dedupe keys.
    if os.path.exists(sqlite_file):
        os.remove(sqlite_file)
    conn = sqlite3.connect(sqlite_file)
    cur = conn.cursor()
    cur.execute("CREATE TABLE seen (key TEXT PRIMARY KEY)")
    conn.commit()

    inserted = 0
    total_rows = 0

    with open(output_file, "w", encoding="utf-8", newline="") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(columns)

        for p in files:
            for row in _iter_rows(p):
                total_rows += 1
                values = [row.get(col, "") for col in columns]
                key = _sha256_for_values(values)
                cur.execute("INSERT OR IGNORE INTO seen (key) VALUES (?)", (key,))
                if cur.rowcount == 1:
                    writer.writerow(values)
                    inserted += 1

            conn.commit()

    conn.close()
    try:
        os.remove(sqlite_file)
    except OSError:
        pass

    return inserted, total_rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Combine outputs/temp_*.csv into a single deduplicated CSV in the project root. "
            "Deduping is by exact full-row match across the unified header set."
        )
    )
    parser.add_argument("--input-dir", default="outputs", help="Directory containing CSV files (default: outputs)")
    parser.add_argument("--pattern", default="temp_*.csv", help="Glob pattern within input-dir (default: temp_*.csv)")
    parser.add_argument(
        "--output-file",
        default=None,
        help="Output CSV path. If omitted, writes to project root with a timestamped name.",
    )

    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    project_root = os.getcwd()
    output_file = args.output_file
    if output_file is None:
        output_file = os.path.join(project_root, f"temp_combined_deduped_{timestamp}.csv")
    sqlite_file = os.path.join(project_root, f".merge_temp_dedup_{timestamp}.sqlite")

    inserted, total_rows = merge_and_dedup(
        input_dir=args.input_dir,
        pattern=args.pattern,
        output_file=output_file,
        sqlite_file=sqlite_file,
    )

    print(
        f"Merged {len(glob.glob(os.path.join(args.input_dir, args.pattern)))} files. "
        f"Total rows read: {total_rows}. Deduped rows written: {inserted}. "
        f"Output: {output_file}"
    )


if __name__ == "__main__":
    main()

