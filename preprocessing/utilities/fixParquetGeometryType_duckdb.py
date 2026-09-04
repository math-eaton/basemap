#!/usr/bin/env python3
"""
Strip the Parquet-native GEOMETRY logical type annotation that mapshaper's
`format=parquet` writer attaches to the geometry column (see
planetiler/scripts/mapshaper_group_simplify.sh), while preserving the file's
GeoParquet "geo" key-value metadata (encoding=WKB) untouched.

Why this is needed: planetiler.jar (0.10.2, planetiler/java/lib/) requires a
WKB-encoded geometry column to be a bare BINARY/BYTE_ARRAY type with NO
logical type annotation. mapshaper's writer emits the column as BYTE_ARRAY
*plus* a GeometryType logical annotation (physically still raw WKB bytes,
just double-tagged per the newer Parquet-native geometry convention) --
planetiler rejects that combination outright:

    FileFormatException: Binary type required for wkb-encoded geometry
    column geometry got: optional binary geometry (GEOMETRY)

This only affects files that actually went through mapshaper's parquet
writer (the admin/settlement recipes in mapshaper_group_simplify.sh). Other
files -- including ones with a non-"geometry" primary column name, e.g. the
Esri-sourced Africa-wide extents' "Shape" column -- are detected as already
fine and just copied through untouched rather than being rewritten, so this
is safe (if wasteful of a few seconds per file) to run over an entire
directory regardless of which files actually need it. The primary geometry
column name is read from each file's own GeoParquet "geo" metadata, not
assumed to be "geometry".

Usage:
  python fixParquetGeometryType_duckdb.py input.parquet output.parquet
  python fixParquetGeometryType_duckdb.py input_dir/ output_dir/   # all *.parquet in dir

Requires: pip install duckdb (needs the "spatial" extension, auto-installed)
"""

import json
import os
import shutil
import sys
import time
from pathlib import Path

import duckdb


def fmt(t0):
    s = time.time() - t0
    return f"{s / 60:.1f}m" if s >= 60 else f"{s:.1f}s"


def copy_fast(src: Path, dst: Path):
    """Hardlink when possible (same filesystem); fall back to a real copy."""
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def fix_file(con, src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)

    kv = con.execute(f"SELECT key, value FROM parquet_kv_metadata('{src.as_posix()}')").fetchall()
    geo_meta = next((bytes(v).decode() for k, v in kv if bytes(k) == b"geo"), None)
    if geo_meta is None:
        raise ValueError(f"{src}: no GeoParquet 'geo' key-value metadata found -- not a GeoParquet file?")
    geom_col = json.loads(geo_meta)["primary_column"]

    logical_type = con.execute(
        f"SELECT logical_type FROM parquet_schema('{src.as_posix()}') WHERE name = ?", [geom_col]
    ).fetchone()
    if logical_type is None or logical_type[0] is None:
        # Already a bare BINARY column (or duckdb reports it as such) --
        # nothing to strip, so avoid rewriting a potentially multi-GB file.
        copy_fast(src, dst)
        return False

    # DuckDB's COPY ... KV_METADATA option takes a single-quoted string
    # literal, so escape embedded single quotes the standard SQL way.
    geo_meta_escaped = geo_meta.replace("'", "''")
    # The column name comes from the file's own metadata, not a literal, so
    # it's quoted as an identifier rather than interpolated as trusted SQL.
    geom_col_ident = '"' + geom_col.replace('"', '""') + '"'

    con.execute(f"""
        COPY (
            SELECT * REPLACE (ST_AsWKB({geom_col_ident})::BLOB AS {geom_col_ident})
            FROM read_parquet('{src.as_posix()}')
        ) TO '{dst.as_posix()}' (FORMAT PARQUET, KV_METADATA {{'geo': '{geo_meta_escaped}'}});
    """)

    check = con.execute(
        f"SELECT logical_type FROM parquet_schema('{dst.as_posix()}') WHERE name = ?", [geom_col]
    ).fetchone()
    if check and check[0] is not None:
        raise RuntimeError(f"{dst}: geometry column still carries a logical type annotation ({check[0]})")
    return True


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)

    src_arg, dst_arg = Path(sys.argv[1]), Path(sys.argv[2])

    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")

    if src_arg.is_dir():
        files = sorted(src_arg.glob("*.parquet"))
        if not files:
            print(f"No .parquet files found in {src_arg}")
            sys.exit(1)
        for src in files:
            dst = dst_arg / src.name
            t = time.time()
            print(f"{src.name}...", end=" ", flush=True)
            fixed = fix_file(con, src, dst)
            print(f"{'fixed' if fixed else 'already fine, copied'} ({fmt(t)})", flush=True)
    else:
        t = time.time()
        fixed = fix_file(con, src_arg, dst_arg)
        print(f"{src_arg} -> {dst_arg}: {'fixed' if fixed else 'already fine, copied'} ({fmt(t)})")


if __name__ == "__main__":
    main()
