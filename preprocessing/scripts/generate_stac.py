"""
generate_stac.py — Generate STAC Item JSON sidecars for PMTiles archives.

For each .pmtiles file in the output tree:
  1. Reads PMTiles metadata via `pmtiles show --json`.
  2. Parses the structured version JSON embedded in the description field.
  3. Writes a {archive}.stac.json sidecar conforming to STAC 1.0.0.

The STAC Item `id` is derived from the archive stem plus the tile_generated date,
making each generation run produce a unique, sortable identifier.

Media type: application/vnd.pmtiles  (community-accepted per the PMTiles spec)

Usage (standalone):
    python generate_stac.py [--tile-dir /tmp/grid3_tiles/data/3-pmtiles/grid3]
                             [--overwrite] [--verbose]

Usage (Python API):
    from generate_stac import generate_stac_items
    items = generate_stac_items(tile_dir=OUTPUT_GRID3_DIR, verbose=True)
"""

import subprocess
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone

PMTILES_MEDIA_TYPE = "application/vnd.pmtiles"


def _pmtiles_show(path: Path) -> dict:
    """Return the parsed JSON header from a PMTiles archive, or {} on failure."""
    try:
        out = subprocess.check_output(
            ["pmtiles", "show", "--json", str(path)], stderr=subprocess.DEVNULL
        )
        return json.loads(out)
    except Exception:
        return {}


def _parse_version_meta(description: str) -> dict:
    """
    Extract the structured JSON block appended by _build_description() in tippecanoe.py.

    The description field looks like:
        "Human readable text. {...json...}"
    Returns the parsed dict (or {} if absent/unparseable).
    """
    if not description:
        return {}
    brace = description.rfind("{")
    if brace == -1:
        return {}
    try:
        return json.loads(description[brace:])
    except json.JSONDecodeError:
        return {}


def _bbox_to_polygon(bbox: list[float]) -> dict:
    """Convert [west, south, east, north] bbox to GeoJSON Polygon."""
    w, s, e, n = bbox
    return {
        "type": "Polygon",
        "coordinates": [[
            [w, s], [e, s], [e, n], [w, n], [w, s]
        ]],
    }


def _extract_iso3(stem: str) -> str | None:
    """Extract ISO3 code from a stem like GRID3_COD_boundaries → 'COD'."""
    parts = stem.split("_")
    return parts[1] if len(parts) >= 3 else None


def build_stac_item(pmtiles_path: Path) -> dict | None:
    """
    Build a STAC 1.0.0 Item for a PMTiles archive.

    Returns the Item dict, or None if the archive can't be read.
    """
    meta = _pmtiles_show(pmtiles_path)
    if not meta:
        return None

    description = meta.get("description", "")
    version_meta = _parse_version_meta(description)
    human_desc = description[:description.rfind("{")].strip() if "{" in description else description

    # ── Bounding box ──────────────────────────────────────────────────────────
    bounds = meta.get("bounds")           # [west, south, east, north]
    if bounds and len(bounds) == 4:
        bbox     = bounds
        geometry = _bbox_to_polygon(bbox)
    else:
        bbox     = [-180, -90, 180, 90]
        geometry = _bbox_to_polygon(bbox)

    # ── Temporal fields ───────────────────────────────────────────────────────
    published      = version_meta.get("published")          # "YYYY-MM-DD"
    tile_generated = version_meta.get("tile_generated")     # "YYYY-MM-DDThh:mm:ssZ"

    if published:
        try:
            dt_str = datetime.strptime(published, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00Z")
        except ValueError:
            dt_str = tile_generated or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        dt_str = tile_generated or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── Item ID ───────────────────────────────────────────────────────────────
    # Stem + generation date → unique, sortable across re-processing runs.
    gen_date = (tile_generated or dt_str)[:10].replace("-", "")  # YYYYMMDD
    item_id  = f"{pmtiles_path.stem}_{gen_date}"

    # ── Properties ────────────────────────────────────────────────────────────
    iso3 = _extract_iso3(pmtiles_path.stem)
    props: dict = {
        "datetime":  dt_str,
        "title":     meta.get("name") or pmtiles_path.stem,
        "minzoom":   meta.get("minzoom"),
        "maxzoom":   meta.get("maxzoom"),
    }
    if human_desc:
        props["description"] = human_desc
    if version_meta.get("version"):
        props["version"] = version_meta["version"]
    if version_meta.get("dois"):
        props["dois"] = version_meta["dois"]
    elif version_meta.get("doi"):
        props["doi"] = version_meta["doi"]
    if tile_generated:
        props["tile_generated"] = tile_generated
    if iso3:
        props["iso3"] = iso3.upper()

    # ── Links ─────────────────────────────────────────────────────────────────
    links = []
    dois_list = version_meta.get("dois") or []
    if dois_list:
        for entry in dois_list:
            href = entry.get("url") or f"https://doi.org/{entry.get('id', '')}"
            if href:
                links.append({
                    "rel":   "via",
                    "href":  href,
                    "type":  "text/html",
                    "title": f"Source DOI: {entry.get('id', href)}",
                })
    else:
        doi = version_meta.get("doi") or meta.get("attribution", "")
        if doi and doi.startswith("http"):
            links.append({
                "rel":   "via",
                "href":  doi,
                "type":  "text/html",
                "title": "Source DOI",
            })

    # ── Assets ────────────────────────────────────────────────────────────────
    size_bytes = pmtiles_path.stat().st_size if pmtiles_path.exists() else None
    asset: dict = {
        "href":  f"./{pmtiles_path.name}",
        "type":  PMTILES_MEDIA_TYPE,
        "title": "PMTiles archive",
        "roles": ["data"],
    }
    if size_bytes is not None:
        asset["file:size"] = size_bytes

    return {
        "type":          "Feature",
        "stac_version":  "1.0.0",
        "id":            item_id,
        "bbox":          bbox,
        "geometry":      geometry,
        "datetime":      dt_str,
        "properties":    props,
        "links":         links,
        "assets":        {"data": asset},
    }


def generate_stac_items(
    tile_dir: Path | str,
    overwrite: bool = False,
    verbose: bool = True,
) -> list[dict]:
    """
    Generate STAC Item JSON sidecars for all .pmtiles files under tile_dir.

    Writes {archive}.stac.json alongside each archive.

    Args:
        tile_dir:  Root directory to search (recursively).
        overwrite: Replace existing .stac.json files (default: skip existing).
        verbose:   Print progress.

    Returns:
        List of result dicts: {pmtiles, stac_json, success, skipped}.
    """
    tile_dir = Path(tile_dir)
    pmtiles  = sorted(tile_dir.rglob("*.pmtiles"))

    if verbose:
        print(f"=== STAC ITEM GENERATION ===")
        print(f"  Source: {tile_dir}  ({len(pmtiles)} archive(s))\n")

    results = []
    for p in pmtiles:
        stac_path = p.with_suffix(".stac.json")

        if stac_path.exists() and not overwrite:
            if verbose:
                print(f"  – {p.name}: sidecar exists, skipping")
            results.append({"pmtiles": str(p), "stac_json": str(stac_path),
                            "success": True, "skipped": True})
            continue

        item = build_stac_item(p)
        if item is None:
            if verbose:
                print(f"  ✗ {p.name}: could not read PMTiles metadata")
            results.append({"pmtiles": str(p), "stac_json": str(stac_path),
                            "success": False, "skipped": False})
            continue

        stac_path.write_text(json.dumps(item, indent=2))
        if verbose:
            print(f"  ✓ {stac_path.name}")
        results.append({"pmtiles": str(p), "stac_json": str(stac_path),
                        "success": True, "skipped": False})

    if verbose:
        ok   = sum(1 for r in results if r["success"] and not r["skipped"])
        skip = sum(1 for r in results if r["skipped"])
        fail = sum(1 for r in results if not r["success"])
        print(f"\n  Done: {ok} generated, {skip} skipped, {fail} failed")

    return results


def main():
    parser = argparse.ArgumentParser(description="Generate STAC Item JSON sidecars for PMTiles")
    parser.add_argument("--tile-dir", default=None,
                        help="Tile output directory (default: OUTPUT_GRID3_DIR from config)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing .stac.json sidecars")
    parser.add_argument("--verbose", action="store_true", default=True)
    args = parser.parse_args()

    if args.tile_dir:
        tile_dir = Path(args.tile_dir)
    else:
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from config import OUTPUT_GRID3_DIR
        tile_dir = OUTPUT_GRID3_DIR

    generate_stac_items(tile_dir=tile_dir, overwrite=args.overwrite, verbose=args.verbose)


if __name__ == "__main__":
    main()
