// GRID3 "latest" tileset — hand-written Planetiler profile, replacing
// planetiler/schema/GRID3_latest.yaml because the YAML custommap DSL has no
// "parquet" source type (confirmed against DataSourceType.java — only osm,
// shapefile, geopackage, geojson are valid there), and the pipeline is moving
// to GeoParquet inputs (see run.sh conversion note below).
//
// Modeled on https://github.com/OvertureMaps/overture-tiles/blob/main/profiles/
// (OvertureProfile.java / Divisions.java) rather than the older
// pmtiles/tiles/src/main/java/com/protomaps/basemap/ code, per instruction —
// that project is OSM-oriented and out of date for this use case. Like
// Overture's profiles, this is a single-file Java program (JEP 330) — no
// Maven build needed, just:
//
//   java -cp planetiler/java/lib/planetiler.jar planetiler/java/GRID3.java \
//     --data_dir=/tmp/grid3_tiles/data/2-scratch/grid3/cod
//
// Unlike Overture's one-theme-many-hive-partitioned-subtypes model, GRID3's
// sources are independently-schema'd parquet files (one per admin level /
// settlement dataset / roads / etc.), so this mirrors the composition style
// of the *old* Basemap.java instead: one ForwardingProfile with a
// registerSourceHandler(sourceName, handler) per source — just with today's
// parquet-based Planetiler APIs, not OSM-specific logic.
//
// IMPORTANT — no CRS override for parquet: addParquetSource() has no
// projection parameter (unlike addGeoPackageSource/addShapefileSource).
// GeoParquet sources rely entirely on their own embedded CRS metadata. Our
// data is EPSG:4326 throughout and any standard GeoParquet writer (GDAL,
// DuckDB, mapshaper) embeds that correctly by default, so this should just
// work — but there is no planetiler-side safety net if a conversion step
// ever writes bad/missing CRS metadata; that has to be caught upstream.
//
// Translated 1:1 from planetiler/schema/GRID3_latest.yaml — see that file's
// header comments for the reasoning behind each zoom/tolerance/exclude choice
// (nested-admin tolerance grading, settlement blocks-vs-extents two-tier
// split, roads zoom-stepping by class, COD/NGA exclusion from the Africa
// layer, etc.). This file only notes where the Java translation differs.

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.ForwardingProfile;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;

import java.nio.file.Path;
import java.util.List;

public class GRID3 extends ForwardingProfile {

  // ── Output vector tile layer ids (unchanged from GRID3_latest.yaml, so
  //    pmtiles/app/src/style.json's existing paint rules keep working) ──────
  private static final String PROVINCE = "GRID3-COD-province-v9-0";
  private static final String PROVINCE_CENTROIDS = "GRID3-COD-province-v9-0-centroids";
  private static final String ANTENNE = "GRID3-COD-antenne-v9-0";
  private static final String ANTENNE_CENTROIDS = "GRID3-COD-antenne-v9-0-centroids";
  private static final String ZONESANTE = "GRID3-COD-zonesante-v9-0";
  private static final String ZONESANTE_CENTROIDS = "GRID3-COD-zonesante-v9-0-centroids";
  private static final String AIRESANTE = "GRID3-COD-airesante-v9-0";
  private static final String AIRESANTE_CENTROIDS = "GRID3-COD-airesante-v9-0-centroids";
  private static final String COD_SETTLEMENT_BLOCKS = "GRID3-COD-settlement-blocks-v4-0";
  private static final String COD_SETTLEMENT_EXTENTS = "GRID3-COD-settlement-extents-v4-0";
  private static final String AF_SETTLEMENT_EXTENTS = "GRID3-AFRICA-settlement-extents-v3-0";
  private static final String HEALTH_FACILITIES = "GRID3-COD-health-facilities-v9-0";
  private static final String SETTLEMENT_NAMES = "GRID3-COD-settlement-names-v9-0";
  private static final String ROADS = "GRID3-COD-roads-v1-0";

  public GRID3() {
    registerSourceHandler("cod_province", this::processProvince);
    registerSourceHandler("cod_antenne", this::processAntenne);
    registerSourceHandler("cod_zonesante", this::processZonesante);
    registerSourceHandler("cod_airesante", this::processAiresante);
    registerSourceHandler("cod_settlement_blocks", this::processSettlementBlocks);
    registerSourceHandler("cod_settlement_extents", this::processSettlementExtents);
    registerSourceHandler("cod_health_facilities", this::processHealthFacilities);
    registerSourceHandler("cod_settlement_names", this::processSettlementNames);
    registerSourceHandler("cod_roads", this::processRoads);
    registerSourceHandler("af_settlement_extents", this::processAfricaSettlementExtents);
    registerHandler(new RoadsPostProcessor());
  }

  // ── Administrative boundaries ──────────────────────────────────────────────
  // DOIs: 10.7916/06qw-9y10, 10.7916/hv5g-p227 (published 2026-01-13)

  private void processProvince(SourceFeature source, FeatureCollector features) {
    copyAttrs(features.polygon(PROVINCE).setMinZoom(3), source,
      "province", "pays", "iso3", "prov_uid", "date", "edit_par", "grid3id");

    var centroid = features.centroidIfConvex(PROVINCE_CENTROIDS).setMinZoom(3);
    copyAttrs(centroid, source, "province", "pays", "iso3", "prov_uid", "date", "edit_par", "grid3id");
    setAreaKm2(centroid, source);
  }

  private void processAntenne(SourceFeature source, FeatureCollector features) {
    copyAttrs(features.polygon(ANTENNE).setMinZoom(3), source,
      "antenne", "pays", "iso3", "province", "prov_uid", "date", "edit_par", "grid3id");

    var centroid = features.centroidIfConvex(ANTENNE_CENTROIDS).setMinZoom(3);
    copyAttrs(centroid, source, "antenne", "pays", "iso3", "province", "prov_uid", "date", "edit_par", "grid3id");
    setAreaKm2(centroid, source);
  }

  private void processZonesante(SourceFeature source, FeatureCollector features) {
    var poly = features.polygon(ZONESANTE).setMinZoom(5).setPixelTolerance(0.2);
    copyAttrs(poly, source, "pays", "iso3", "province", "prov_uid", "antenne", "zonesante", "zs_uid",
      "date", "source_acronym", "sourceid", "edit_par", "grid3id");

    var centroid = features.centroidIfConvex(ZONESANTE_CENTROIDS).setMinZoom(5);
    copyAttrs(centroid, source, "pays", "iso3", "province", "prov_uid", "antenne", "zonesante", "zs_uid",
      "date", "source_acronym", "sourceid", "edit_par", "grid3id");
    setAreaKm2(centroid, source);
  }

  private void processAiresante(SourceFeature source, FeatureCollector features) {
    var poly = features.polygon(AIRESANTE).setMinZoom(7)
      .setPixelTolerance(0.5).setPixelToleranceAtMaxZoom(0.1);
    copyAttrs(poly, source, "pays", "iso3", "province", "prov_uid", "antenne", "zonesante", "zs_uid",
      "airesante", "as_uid", "asnom_alt", "date", "source_acronym", "sourceid", "edit_par", "grid3id");

    var centroid = features.centroidIfConvex(AIRESANTE_CENTROIDS).setMinZoom(7);
    copyAttrs(centroid, source, "pays", "iso3", "province", "prov_uid", "antenne", "zonesante", "zs_uid",
      "airesante", "as_uid", "asnom_alt", "date", "source_acronym", "sourceid", "edit_par", "grid3id");
    setAreaKm2(centroid, source);
  }

  // ── Settlement extents / blocks ────────────────────────────────────────────
  // DOI: 10.7916/bb5b-9b79 (v4.0, published 2026-04-03) for both layers below.
  // See GRID3_latest.yaml for the full two-tier rationale (blocks = raw
  // per-block data z11+, extents = dissolved-by-mgrs_code overview z7+,
  // produced by preprocessing/utilities/dissolveBlocks_COD_duckdb.py).

  private static final String[] SETTLEMENT_BLOCK_ATTRS = {
    "block_id", "country", "iso3", "extent_type", "mgrs_code", "composite_class",
    "block_area_sqm", "block_perimeter", "building_count",
    "building_area_sum", "building_area_min", "building_area_max", "building_area_median",
    "building_area_stdev", "building_area_density", "building_count_density",
    "building_count_density_quantile_rank", "building_max_area_quantile_rank",
    "block_neighbor_count", "blocks_per_settl_extent", "ndvi_mean", "evi_mean",
    "gbuilding_max_height", "gbuilding_mean_height"
  };

  private void processSettlementBlocks(SourceFeature source, FeatureCollector features) {
    // Conservative on purpose: no min-pixel-size here — dropping a whole block
    // feature would punch a visible hole in what's supposed to read as a
    // contiguous block mosaic.
    var poly = features.polygon(COD_SETTLEMENT_BLOCKS).setMinZoom(11)
      .setPixelTolerance(0.5).setPixelToleranceAtMaxZoom(0.1);
    copyAttrs(poly, source, SETTLEMENT_BLOCK_ATTRS);
  }

  private void processSettlementExtents(SourceFeature source, FeatureCollector features) {
    // Generalized overview: more aggressive tolerance than the blocks it's
    // dissolved from, plus a min pixel size to drop slivers that are noise at
    // z7-10 scale — full detail restored at max zoom for overzooming.

    String type = source.getString("extent_type", "");
    int minZoom = switch (type) {
      case "Small Settlement Area" -> 9;
      case "Hamlet" -> 12;
      default -> 7;
    };


    var poly = features.polygon(COD_SETTLEMENT_EXTENTS).setMinZoom(minZoom)
      .setPixelTolerance(0.5).setPixelToleranceAtMaxZoom(0.1)
      .setMinPixelSizeAtAllZooms(2).setMinPixelSizeAtMaxZoom(0);
    copyAttrs(poly, source, "mgrs_code", "country", "iso3", "extent_type", "composite_class",
      "block_area_sqm", "block_perimeter", "building_count",
      "building_area_sum", "building_area_min", "building_area_max", "building_area_median",
      "building_area_stdev", "building_area_density", "building_count_density",
      "block_neighbor_count", "ndvi_mean", "evi_mean",
      "gbuilding_max_height", "gbuilding_mean_height", "dissolved_block_count");
  }

  // ── Africa-wide settlement extents ─────────────────────────────────────────
  // DOI: unknown/TBD (see GRID3_latest.yaml). COD and NGA are excluded — they
  // have their own newer datasets above. Filters on `country` (values
  // 'COD'/'NGA') per instruction; the source also has a separate `iso3` field
  // if that turns out to be the one that actually holds those codes.
  // min_zoom stepped by `type`: Small Settlement Area -> z9, Hamlet -> z12,
  // everything else (incl. Built-up Area) -> z7 — same values as the old COD
  // v3.1 extents layer this dataset's field names mirror.

  private void processAfricaSettlementExtents(SourceFeature source, FeatureCollector features) {
    String country = source.getString("country", "");
    if (country.equals("COD") || country.equals("NGA")) {
      return;
    }

    String type = source.getString("type", "");
    int minZoom = switch (type) {
      case "Small Settlement Area" -> 9;
      case "Hamlet" -> 12;
      default -> 7;
    };

    var poly = features.polygon(AF_SETTLEMENT_EXTENTS).setMinZoom(minZoom)
      .setPixelTolerance(0.5).setPixelToleranceAtMaxZoom(0.1)
      .setMinPixelSizeAtAllZooms(2).setMinPixelSizeAtMaxZoom(0);
    copyAttrs(poly, source, "country", "iso3", "type", "mgrs_code",
      "building_count", "building_area", "probability", "date", "source");
  }

  // ── Points of interest ─────────────────────────────────────────────────────
  // DOIs: 10.7916/77t0-h465 (health facilities), 10.7916/k9sy-hc13 (settlement
  // names), published 2026-01-13. `lat`/`lon`/`precision_` are dropped —
  // redundant with the point geometry itself.

  private void processHealthFacilities(SourceFeature source, FeatureCollector features) {
    // Stepped by esstype, reusing tile_layer_steps.json's
    // health_facilities_esstype windows. "otherwise" falls back to the
    // layer's base z5 rather than being silently dropped (a deliberate
    // deviation from the tippecanoe pipeline — see GRID3_latest.yaml).
    String esstype = source.getString("esstype", "");
    int minZoom = switch (esstype) {
      case "Bureau Central de la Zone de Santé", "Hôpital Général de Référence" -> 8;
      case "Hôpital", "Centre Hopitalier" -> 9;
      case "Centre Médical", "Centre de Santé de Référence", "Centre de Santé", "Poste de Santé" -> 10;
      case "Clinique", "Polyclinque", "Dispensaire", "Site Soin Communautaire", "Autre" -> 13;
      default -> 10;
    };

    var point = features.point(HEALTH_FACILITIES).setMinZoom(minZoom);
    copyAttrs(point, source, "pays", "iso3", "province", "prov_uid", "antenne", "zonesante", "zs_uid",
      "airesante", "as_uid", "localite", "essnom1", "essnom2", "esstype", "typeorig", "dhis2",
      "categorie", "vaccfixe", "frigo", "frigofct", "date", "source_acronym", "origine",
      "grid3id", "sourceid");
  }

  private void processSettlementNames(SourceFeature source, FeatureCollector features) {
    var point = features.point(SETTLEMENT_NAMES).setMinZoom(11);
    copyAttrs(point, source, "pays", "iso3", "province", "prov_uid", "antenne", "zonesante", "zs_uid",
      "airesante", "as_uid", "localite", "localitetype", "localite_alt", "enclav", "enclavdate",
      "source_acronym", "date", "grid3id", "sourceid");
  }

  // ── Roads ───────────────────────────────────────────────────────────────────
  // DOI: 10.7916/00gb-e164 (v1.0, published 2026-01-14). `ORIG_FID` is
  // dropped (Esri row-index artifact). min_zoom stepped by `class`, reusing
  // tile_layer_steps.json's "roads" windows, restructured so track/path/
  // unclassified/residential/etc. (and any future/unlisted class) all render
  // from z11 — see GRID3_latest.yaml. Line merging is handled by
  // RoadsPostProcessor below, not per-feature tolerance/min-size (matching
  // the YAML's tile_post_process, which similarly supersedes per-feature
  // settings once defined for a layer).

  private void processRoads(SourceFeature source, FeatureCollector features) {
    String cls = source.getString("class", "");
    int minZoom = switch (cls) {
      case "service", "steps" -> 13;
      case "tertiary" -> 10;
      case "secondary" -> 7;
      case "primary" -> 6;
      case "motorway", "trunk" -> 5;
      default -> 11;
    };

    var line = features.line(ROADS).setMinZoom(minZoom);
    copyAttrs(line, source, "grid3_id", "gers", "country", "iso3", "source_id", "class", "subclass",
      "names", "label", "road_surface", "speed_estimate", "speed_estimate_method", "speed_limits",
      "date", "source_acronym");
  }

  /** Fuses touching same-attribute road segments (this dataset carries Overture
   * GERS-style over-segmentation) and drops merged segments under ~2 tile
   * pixels below max zoom, keeping full detail at max zoom for overzooming —
   * same min_length/tolerance/buffer values as tile_post_process.merge_line_strings
   * in GRID3_latest.yaml. */
  private static final class RoadsPostProcessor implements LayerPostProcessor {
    @Override
    public String name() {
      return ROADS;
    }

    @Override
    public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) throws GeometryException {
      double minLength = zoom >= MAXZOOM ? 0 : 2;
      double tolerance = zoom >= MAXZOOM ? 0.1 : 0.375;
      return FeatureMerge.mergeLineStrings(items, minLength, tolerance, 4, true);
    }
  }

  // ── Helpers ─────────────────────────────────────────────────────────────────

  private static void copyAttrs(FeatureCollector.Feature feature, SourceFeature source, String... keys) {
    for (String key : keys) {
      feature.setAttr(key, source.getTag(key));
    }
  }

  /** Bounding-box area in km² (not true polygon area) — matches the YAML's
   * feature.bbox.area("km2"), which was a deliberate design choice there, not
   * a workaround. Computed from the ORIGINAL source geometry's envelope
   * regardless of what output geometry this feature emits (e.g. centroidIfConvex). */
  private static void setAreaKm2(FeatureCollector.Feature feature, SourceFeature source) {
    try {
      var envelope = GeoUtils.JTS_FACTORY.toGeometry(source.worldGeometry().getEnvelopeInternal());
      feature.setAttr("bbox_km2", GeoUtils.areaInMeters(envelope) / 1_000_000.0);
    } catch (GeometryException e) {
      // leave unset if the geometry can't be measured
    }
  }

  // ── Profile metadata ────────────────────────────────────────────────────────

  @Override
  public String name() {
    return "GRID3 Latest";
  }

  @Override
  public String description() {
    return "GRID3 DRC (COD) administrative, settlement, health-facility, and roads layers, " +
      "plus the sub-continental Africa-wide settlement extents overview layer.";
  }

  @Override
  public String attribution() {
    return "© The Trustees of Columbia University in the City of New York. CC BY-SA 4.0.";
  }

  @Override
  public boolean isOverlay() {
    return true;
  }

  // ── Entry point ─────────────────────────────────────────────────────────────
  // Mirrors OvertureProfile.run(): each source is a single parquet file (no
  // hive partitioning, unlike Overture's theme=/type= layout), so
  // addParquetSource(name, List.of(path)) is called once per GRID3 source.

  private static final int MAXZOOM = 15;

  public static void main(String[] args) throws Exception {
    var arguments = Arguments.fromArgsOrConfigFile(args).orElse(Arguments.of("maxzoom", MAXZOOM));
    String dataDir = arguments.getString("data_dir",
      "Directory of GeoParquet source data (converted from the source .fgb/.gpkg files)",
      "/tmp/grid3_tiles/data/2-scratch/grid3/cod");
    Path dir = Path.of(dataDir);

    Planetiler.create(arguments)
      .setProfile(new GRID3())
      .addParquetSource("cod_province", List.of(dir.resolve("GRID3_COD_province_v9_0.parquet")))
      .addParquetSource("cod_antenne", List.of(dir.resolve("GRID3_COD_antenne_v9_0.parquet")))
      .addParquetSource("cod_zonesante", List.of(dir.resolve("GRID3_COD_zonesante_v9_0.parquet")))
      .addParquetSource("cod_airesante", List.of(dir.resolve("GRID3_COD_airesante_v9_0.parquet")))
      .addParquetSource("cod_settlement_blocks", List.of(dir.resolve("GRID3_COD_settlement_blocks_v4_0.parquet")))
      .addParquetSource("cod_settlement_extents", List.of(dir.resolve("GRID3_COD_settlement_extents_v4_0.parquet")))
      .addParquetSource("cod_health_facilities", List.of(dir.resolve("GRID3_COD_health_facilities_v9_0.parquet")))
      .addParquetSource("cod_settlement_names", List.of(dir.resolve("GRID3_COD_settlement_names_v9_0.parquet")))
      .addParquetSource("cod_roads", List.of(dir.resolve("GRID3_COD_roads_v1_0.parquet")))
      .addParquetSource("af_settlement_extents",
        List.of(dir.resolve("GRID3_AF_settlement_extents_v3_0.parquet")))
      .overwriteOutput(Path.of("GRID3_latest.pmtiles"))
      .run();
  }
}
