#!/bin/bash

docker run -e JAVA_TOOL_OPTIONS="-Xmx64g"  -v "$(pwd)":/data ghcr.io/onthegomap/planetiler:latest generate-custom --schema=/data/schema.yml --tile-format=mlt --mlt-shared-dict --mlt-tessellate-polygons --mlt-reorder-features --output_layerstats=true --output=/data/GRID3_COD.pmtiles --data_dir=/data 