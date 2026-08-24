#!/usr/bin/env bash
# Topology-preserving group-simplification recipes for GRID3 layers, using
# mapshaper's shared-topology model: `-i a b c combine-files` imports several
# files into one shared pool of boundary arcs, and every command chained
# after it in the same invocation (including `-simplify`) operates on that
# shared pool — so edges that coincide across layers (nested admin levels,
# or a dissolved "extents" polygon and the "blocks" it was unioned from)
# simplify together and stay aligned, instead of drifting apart the way
# planetiler's independent per-layer simplification would.
#
# This is meant as a LIGHT pre-tiling pass, not a replacement for the
# per-zoom `tolerance`/`tolerance_at_max_zoom` already tuned in
# planetiler/schema/GRID3_latest.yaml — that still runs per zoom at tile-serve
# time. Keep the percentages here moderate for the admin/blocks recipes;
# only the continent-wide Africa recipe leans aggressive, since it's a
# generalized z7+ overview of 9.28M input polygons.
#
# Requires: npm install -g mapshaper (or `npx mapshaper`)
# For the large files (settlement blocks: 1.15M features; Africa: 9.28M),
# bump Node's heap if you see "JavaScript heap out of memory":
#   NODE_OPTIONS=--max-old-space-size=8192 ./mapshaper_group_simplify.sh admin
#
# Usage: ./mapshaper_group_simplify.sh {admin|settlement|africa} [data_dir] [out_dir]

set -euo pipefail

recipe="${1:?Usage: $0 admin|settlement|africa [data_dir] [out_dir]}"
data_dir="${2:-/tmp/grid3_tiles/data/2-scratch/grid3/cod/gpkg}"
out_dir="${3:-$data_dir/simplified}"
mkdir -p "$out_dir"

case "$recipe" in

  # ── Nested admin boundaries: province / antenne / zonesante / airesante ────
  # Assumes each coarser level is a literal dissolve of the level below it
  # (same source vertices at shared edges) — verify visually after the first
  # run: a province edge and the antenne edge it borders should still align.
  admin)
    mapshaper \
      -i "$data_dir/GRID3_COD_province_v9_0.gpkg" \
         "$data_dir/GRID3_COD_antenne_v9_0.gpkg" \
         "$data_dir/GRID3_COD_zonesante_v9_0.gpkg" \
         "$data_dir/GRID3_COD_airesante_v9_0.gpkg" \
         combine-files \
      -snap \
      -clean \
      -simplify 25% weighted keep-shapes \
      -o "$out_dir/" 'target=*' format=geopackage
    ;;

  # ── COD settlement extents + block subdivisions ────────────────────────────
  # Run preprocessing/utilities/dissolveBlocks_COD_duckdb.py FIRST to produce
  # the extents file from the blocks — this recipe simplifies the two
  # together afterward so a block's outer edge stays coincident with the
  # extents polygon (its mgrs_code cell) it's dissolved into.
  settlement)
    mapshaper \
      -i "$data_dir/GRID3_COD_settlement_blocks_v4_0.gpkg" \
         "$data_dir/GRID3_COD_settlement_extents_v4_0.gpkg" \
         combine-files \
      -snap \
      -clean \
      -simplify 30% weighted keep-shapes \
      -o "$out_dir/" 'target=*' format=geopackage
    ;;

  # ── Africa-wide settlement extents (no blocks layer for this one) ──────────
  # Single layer, so no combine-files/shared-topology step is needed — this
  # is just a standard (still topology-safe within the one layer) simplify.
  # More aggressive % than the recipes above: this feeds a z7+ generalized
  # overview only (min_zoom in GRID3_latest.yaml), and planetiler's own
  # min_size/tolerance already drop sub-pixel slivers downstream, so there's
  # little value in preserving fine detail here across 9.28M input polygons.
  africa)
    mapshaper \
      -i "$data_dir/GRID3_AFRICA_settlement_extents_v3_0.gpkg" \
      -simplify 10% weighted keep-shapes \
      -o "$out_dir/GRID3_AFRICA_settlement_extents_v3_0.gpkg" format=geopackage
    ;;

  *)
    echo "Unknown recipe '$recipe' (expected: admin, settlement, africa)" >&2
    exit 1
    ;;
esac

echo "Done: $recipe -> $out_dir"
