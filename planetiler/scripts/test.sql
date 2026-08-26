INSTALL spatial;
LOAD spatial;

COPY (
    SELECT 
        * EXCLUDE (geom),             -- Exclude original blob column if needed
        st_geomfromwkb(geom) AS geometry  -- Convert to DuckDB Geometry type
    FROM st_read('/mnt/d/mheaton/grid3_tiles/data/1-input/grid3/nga/GRID3_NGA_roads_v1_0.gpkg', layer='main_GRID3_NGA_roads_v1')
) 
TO 'GRID3_NGA_roads_v1_0.parquet' 
(FORMAT 'parquet', COMPRESSION 'zstd');
