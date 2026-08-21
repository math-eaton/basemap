#!/usr/bin/env python3
"""
Usage: python dissolveBlocks_COD_duckdb.py input.fgb output.gpkg

Requires: pip install duckdb
"""

import sys
import time
import duckdb

src, dst = sys.argv[1], sys.argv[2]

def fmt(t0):
    s = time.time() - t0
    return f"{s/60:.1f}m" if s >= 60 else f"{s:.1f}s"

t_total = time.time()

con = duckdb.connect()
con.install_extension("spatial")
con.load_extension("spatial")

# Stage 1: load into an in-memory table so we can report row count and reuse
print("Reading source...", flush=True)
t = time.time()
con.execute(f"CREATE TABLE blocks AS SELECT * FROM ST_Read('{src}')")
n_in = (con.execute("SELECT COUNT(*) FROM blocks").fetchone() or (0,))[0]
print(f"  {n_in:,} blocks loaded ({fmt(t)})", flush=True)

# Stage 2: dissolve into a result table.
# extent_type/composite_class use MIN() as a cheap stand-in for "dominant
# type" — not a true mode. block_perimeter is recomputed from the unioned
# geometry (ST_Perimeter) rather than summed from parts, since summing
# per-block perimeters would double-count internal boundaries between
# merged blocks. building_area_median/stdev and blocks_per_settl_extent are
# averaged from the pre-dissolve per-block values, not recomputed from the
# pooled building population.
print("Dissolving by mgrs_code...", flush=True)
t = time.time()
con.execute("""
    CREATE TABLE dissolved AS
    SELECT
        mgrs_code,
        MIN(country)                                             AS country,
        MIN(iso3)                                                AS iso3,
        MIN(extent_type)                                         AS extent_type,
        MIN(composite_class)                                     AS composite_class,
        SUM(block_area_sqm)                                      AS block_area_sqm,
        ST_Perimeter(ST_Union_Agg(geom))                         AS block_perimeter,
        CAST(SUM(block_neighbor_count) AS BIGINT)                AS block_neighbor_count,
        CAST(SUM(building_count) AS BIGINT)                      AS building_count,
        MIN(building_area_min)                                   AS building_area_min,
        MAX(building_area_max)                                   AS building_area_max,
        SUM(building_area_sum)                                   AS building_area_sum,
        AVG(building_area_median)                                AS building_area_median,
        AVG(building_area_stdev)                                 AS building_area_stdev,
        SUM(building_area_sum) / NULLIF(SUM(block_area_sqm), 0) AS building_area_density,
        SUM(building_count) / NULLIF(SUM(block_area_sqm), 0)    AS building_count_density,
        AVG(ndvi_mean)                                           AS ndvi_mean,
        AVG(evi_mean)                                            AS evi_mean,
        MAX(gbuilding_max_height)                                AS gbuilding_max_height,
        AVG(gbuilding_mean_height)                                AS gbuilding_mean_height,
        AVG(blocks_per_settl_extent)                             AS blocks_per_settl_extent,
        CAST(COUNT(*) AS BIGINT)                                 AS dissolved_block_count,
        ST_Union_Agg(geom)                                       AS geom
    FROM blocks
    GROUP BY mgrs_code
""")
n_out = (con.execute("SELECT COUNT(*) FROM dissolved").fetchone() or (0,))[0]
print(f"  {n_out:,} MGRS polygons ({fmt(t)})", flush=True)

# Stage 3: write to GeoPackage
print(f"Writing {dst}...", flush=True)
t = time.time()
con.execute(f"""
    COPY dissolved TO '{dst}'
    WITH (FORMAT GDAL, DRIVER 'GPKG')
""")
print(f"  Written ({fmt(t)})", flush=True)

print(f"\nDone: {n_in:,} blocks -> {n_out:,} MGRS polygons  total {fmt(t_total)}", flush=True)
