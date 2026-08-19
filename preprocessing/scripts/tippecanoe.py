"""
Tippecanoe configuration for layer groups.

PROFILES: named flag sets (boundaries, POI, settlement_extents, roads).
LAYER_GROUPS: loaded lazily from per-ISO3 YAML files in preprocessing/profiles/{iso3}/layers.yaml.
  - polygon_layers / line_layers / point_layers run in separate tippecanoe invocations
    then merged by tile-join in runCreateTiles.py.
  - modifiers: per-file zoom_filter_windows (from tile_layer_steps.json via filter_key).

Shared Boundary Handling:
    --no-polygon-splitting + --no-simplification-of-shared-nodes ensure adjacent
    admin polygons share identical boundary coordinates across tile edges.

Filter notes:
    Zoom-windowed filters are embedded in the -L JSON spec's "filter" key, using
    legacy Mapbox GL filter syntax (["in","class","val"]).  This is tippecanoe's
    native per-layer filter mechanism and is distinct from -j (global feature filter).
    The "*" key in tile_layer_steps.json is a MapLibre GL style expression for
    style-sheet use only — it is NOT passed to tippecanoe.
"""

from pathlib import Path
import json as _json
import datetime as _dt

# ---------------------------------------------------------------------------
# Profiles: named collections of tippecanoe settings for each thematic type.
# These are code, not data — edit here when flag semantics change.
# ---------------------------------------------------------------------------
PROFILES = {
    "boundaries": {
        "description": "Administrative and operational boundary polygons",
        "polygon_settings": [
            "--hilbert",
            "--no-simplification-of-shared-nodes",
            "--simplification=3",
            "--no-tiny-polygon-reduction",
            "--no-feature-limit",
            "--no-polygon-splitting",
            "--no-tile-size-limit",
        ],
        "point_settings": [
            "--cluster-densest-as-needed",
            "--cluster-maxzoom=12",
            "--preserve-point-density-threshold=32",
        ],
    },
    "POI": {
        "description": "Point features (health facilities, settlement names, other toponyms)",
        "settings": [
            "--cluster-densest-as-needed",
            "--cluster-maxzoom=12",
            "--preserve-point-density-threshold=32",
        ],
    },
    "settlement_extents": {
        "description": "Settlement extent polygons",
        "auto_zoom": True,
        "settings": [
            "--hilbert",
            "--simplification=5",
            "--drop-densest-as-needed",
            "--coalesce-smallest-as-needed",
            "--maximum-tile-bytes=1048576",
            "--include=extent_type",
            "--include=type",
            "--include=building_count",
            "--include=building_count_density_quantile_rank",
            "--include=iso3",
            "--include=mgrs_code",
            "--calculate-feature-index",
        ],
    },
    "roads": {
        "description": "Linear road features",
        "auto_zoom": False,
        "line_settings": [
            "--hilbert",
            "--simplification=8",
            # "--simplify-only-low-zooms",
            "--maximum-tile-bytes=1048576",
            "--drop-densest-as-needed"
            # "--no-feature-limit",
        ],
    },
}


# ---------------------------------------------------------------------------
# YAML loader: replaces the old hardcoded LAYER_GROUPS dict.
# Reads preprocessing/profiles/{iso3}/layers.yaml for every ISO3 that has one.
# "africa" uses assembly.yaml (tile-join rules) and is skipped here.
# ---------------------------------------------------------------------------

_PROFILES_DIR = Path(__file__).resolve().parent.parent / "profiles"


def _load_layer_groups() -> dict:
    """
    Load LAYER_GROUPS from per-ISO3 YAML files.

    YAML layers are list-of-dicts; this normalises them to the tuple format
    expected by build_tippecanoe_group_command:
        (filename, layer_name, minzoom, maxzoom)

    A layer entry with a `modifier` field is also registered in the group's
    `modifiers` dict so build_tippecanoe_group_command can find it.
    """
    try:
        import yaml as _yaml
    except ImportError:
        raise RuntimeError(
            "PyYAML is required for loading layer group config. "
            "Install it with: pip install pyyaml"
        )

    groups: dict = {}
    for yml in sorted(_PROFILES_DIR.glob("*/layers.yaml")):
        iso3 = yml.parent.name
        if iso3 == "africa":
            continue
        data = _yaml.safe_load(yml.read_text())
        for gname, gconf in (data.get("groups") or {}).items():
            modifiers: dict = {}
            for key in ("polygon_layers", "point_layers", "line_layers"):
                tuples = []
                for e in gconf.get(key) or []:
                    tuples.append((e["file"], e["layer"], e["minzoom"], e["maxzoom"]))
                    if "modifier" in e:
                        modifiers[e["file"]] = {"filter_key": e["modifier"]}
                gconf[key] = tuples

            # Group-level polygon_settings override (e.g. NGA settlement extents)
            if "polygon_settings" in gconf and isinstance(gconf["polygon_settings"], list):
                pass  # already a list — pass through as-is

            if modifiers:
                gconf.setdefault("modifiers", modifiers)

            groups[gname] = gconf

    return groups


LAYER_GROUPS: dict = _load_layer_groups()


# ---------------------------------------------------------------------------
# Dictionary accessors (lazy, cached).
# ---------------------------------------------------------------------------

_LAYER_METADATA_CACHE = None


def _get_layer_metadata() -> dict:
    global _LAYER_METADATA_CACHE
    if _LAYER_METADATA_CACHE is None:
        p = Path(__file__).parent.parent / "dictionaries" / "layer_metadata.json"
        _LAYER_METADATA_CACHE = _json.load(open(p)) if p.exists() else {}
    return _LAYER_METADATA_CACHE


_TILE_LAYER_STEPS_CACHE = None


def _get_tile_layer_steps() -> dict:
    global _TILE_LAYER_STEPS_CACHE
    if _TILE_LAYER_STEPS_CACHE is None:
        p = Path(__file__).parent.parent / "dictionaries" / "tile_layer_steps.json"
        _TILE_LAYER_STEPS_CACHE = _json.load(open(p)) if p.exists() else {}
    return _TILE_LAYER_STEPS_CACHE


_LAYER_COMPOSITION_CACHE = None


def _get_layer_composition() -> dict:
    global _LAYER_COMPOSITION_CACHE
    if _LAYER_COMPOSITION_CACHE is None:
        p = Path(__file__).parent.parent / "dictionaries" / "layer_composition.json"
        _LAYER_COMPOSITION_CACHE = _json.load(open(p)) if p.exists() else {}
    return _LAYER_COMPOSITION_CACHE


# ---------------------------------------------------------------------------
# Archive sort helper.
# ---------------------------------------------------------------------------

def sort_archives_by_theme(archives) -> list:
    """
    Return archives sorted by their group's theme position in layer_composition.json.

    Archives whose group/theme is absent fall to the end, preserving relative order.
    """
    comp = _get_layer_composition()
    themes = comp.get("themes", [])
    groups = comp.get("groups", {})
    theme_rank = {t: i for i, t in enumerate(themes)}

    def _rank(archive_path):
        stem = Path(archive_path).stem
        theme = groups.get(stem, {}).get("theme")
        return theme_rank.get(theme, len(themes))

    return sorted(archives, key=_rank)


# ---------------------------------------------------------------------------
# Version metadata helper.
# ---------------------------------------------------------------------------

def _build_description(group: dict) -> str:
    """
    Combine the human-readable description with a compact JSON version block.

    The JSON block is appended so `pmtiles show` output is machine-parseable
    for STAC Item generation, while remaining readable as a plain string.

    DOI serialisation: `dois` (list of {id, url} dicts) takes priority over
    the legacy scalar `doi` key so both single and multi-source tiles are
    handled uniformly.
    """
    meta = {k: group[k] for k in ("version", "published") if group.get(k)}
    if group.get("dois"):
        meta["dois"] = group["dois"]
    elif group.get("doi"):
        meta["doi"] = group["doi"]
    meta["tile_generated"] = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    base = (group.get("description") or "").strip()
    return f"{base} {_json.dumps(meta, separators=(',', ':'))}".strip()


# ---------------------------------------------------------------------------
# Command builder.
# ---------------------------------------------------------------------------

def build_tippecanoe_group_command(group_name: str, layer_tuples: list,
                                   output_file: str, layer_kind: str = "polygon",
                                   extent=None) -> list:
    """
    Build a tippecanoe command for a group of named layers using the -L JSON syntax.

    Unlike --named-layer, the -L JSON format supports per-layer minzoom/maxzoom
    control so each admin level only appears in the tiles where it is needed.

    Args:
        group_name:   Key in LAYER_GROUPS.
        layer_tuples: [(filename, layer_name, minzoom, maxzoom, abs_path), ...]
        output_file:  Path to output PMTiles.
        layer_kind:   "polygon", "point", or "line" — selects settings from PROFILES.
        extent:       Optional (xmin, ymin, xmax, ymax) clipping box.

    Returns:
        list: Complete argv for subprocess.
    """
    group = LAYER_GROUPS[group_name]

    # ── Zoom range ────────────────────────────────────────────────────────────
    profile_name = group.get("profile")
    profile_obj  = PROFILES.get(profile_name, {}) if profile_name else {}
    use_auto_zoom = profile_obj.get("auto_zoom", False) or group.get("auto_zoom", False)
    z_min = min((mz for _, _, mz, _, _ in layer_tuples), default=0)
    if use_auto_zoom:
        zoom_flags = [f"-Z{z_min}", "-zg"]
    else:
        z_max = max((mz for _, _, _, mz, _ in layer_tuples), default=16)
        zoom_flags = [f"-Z{z_min}", f"-z{z_max}"]

    # ── Settings resolution ───────────────────────────────────────────────────
    # Priority: profile defaults → group-level overrides (later args win in tippecanoe).
    settings_key = f"{layer_kind}_settings"
    profile_settings: list = []
    if profile_name and profile_name in PROFILES:
        profile = PROFILES[profile_name]
        profile_settings = list(profile.get(settings_key, profile.get("settings", [])))

    group_override = group.get(settings_key, [])
    exclude = set(group.get("profile_exclude", []))
    resolved_settings = [s for s in (profile_settings + group_override) if s not in exclude]

    # ── Base command ──────────────────────────────────────────────────────────
    cmd = ["tippecanoe", "-fo", output_file] + zoom_flags
    cmd.extend(resolved_settings)
    cmd.append("-P")    # --read-parallel: speed up multi-layer invocations

    # ── Tileset-level metadata (name, description with version JSON, attribution) ──
    if group.get("name"):
        cmd += ["-n", group["name"]]
    desc = _build_description(group)
    if desc:
        cmd += ["-N", desc]
    if group.get("attribution"):
        cmd += ["-A", group["attribution"]]

    # ── Layer order from layer_composition.json ───────────────────────────────
    comp_group = _get_layer_composition().get("groups", {}).get(group_name, {})
    layer_order = comp_group.get("layer_order", [])
    if layer_order:
        order_rank = {name: i for i, name in enumerate(layer_order)}
        layer_tuples = sorted(
            layer_tuples,
            key=lambda t: order_rank.get(t[1], len(layer_order)),
        )

    # ── Per-layer -L specs ────────────────────────────────────────────────────
    modifiers  = group.get("modifiers", {})
    layer_meta = _get_layer_metadata()

    for _, layer_name, minzoom, maxzoom, abs_path in layer_tuples:
        spec = {
            "file":    str(abs_path),
            "layer":   layer_name,
            "minzoom": minzoom,
            "maxzoom": maxzoom,
        }
        m = layer_meta.get(layer_name)
        if m:
            spec["description"] = _json.dumps(
                {k: v for k, v in m.items() if v and not k.startswith("_")},
                separators=(",", ":"),
            )
        mod = modifiers.get(abs_path.name)
        windows = mod.get("zoom_filter_windows") if mod else None
        if windows is None and mod and "filter_key" in mod:
            entry = _get_tile_layer_steps().get(mod["filter_key"], {})
            raw = entry.get("zoom_filter_windows", [])
            windows = [{**w, "maxzoom": w.get("maxzoom", maxzoom)} for w in raw]
        if windows:
            for w in windows:
                wspec = {**spec, "minzoom": w["minzoom"], "maxzoom": w["maxzoom"]}
                if "filter" in w:
                    wspec["filter"] = w["filter"]
                cmd.extend(["-L", _json.dumps(wspec)])
        else:
            cmd.extend(["-L", _json.dumps(spec)])

    if extent:
        xmin, ymin, xmax, ymax = extent
        cmd.extend(["--clip-bounding-box", f"{xmin},{ymin},{xmax},{ymax}"])

    return cmd
