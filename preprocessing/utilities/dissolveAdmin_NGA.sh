#!/usr/bin/env bash

set -euo pipefail

# Usage: dissolveAdmin.sh input.fgb output.fgb
# Dissolve by X, keep the first value for each remaining attribute.

# Override SRC_SRS if your source is different.
SRC_SRS="EPSG:4326"

SRC_LAYER=$(basename "$1" .parquet)
DST_LAYER=$(basename "$2" .parquet)

ogr2ogr \
	-f parquet \
	-nln "$DST_LAYER" \
	-nlt MULTIPOLYGON \
	-dialect SQLITE \
	-a_srs "$SRC_SRS" \
	-sql "SELECT MIN(country) AS country, \
		 MIN(iso3) AS iso3, \
		 MIN(state) AS state, \
		 MIN(statecode) AS statecode, \
		 MIN(lga) AS lga, \
		 SUM(multipart_count) AS multipart_count, \
		 MIN(source) AS source, \
		 MIN(date) AS date, \
		 SUM(area_sqkm) AS area_sqkm, \
		 ST_Multi(ST_Union(geometry)) AS geometry \
	FROM \"$SRC_LAYER\" \
	GROUP BY lga" \
	"$2" "$1"
