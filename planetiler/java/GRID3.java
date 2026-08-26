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
    private static final String COD_ROADS = "GRID3-COD-roads-v1-0";
    private static final String NGA_ROADS = "GRID3-NGA-roads-v1-0";

    // NGA operational admin hierarchy (state/lga/ward — 3 tiers vs. COD's 4).
    // Both v2_0 and v3_0 are kept as separate layers: v3_0 is missing some
    // states present in v2_0 (per instruction, to be selectively filtered
    // later), so neither vintage is dropped here.
    // private static final String NGA_STATES_V2 = "GRID3-NGA-operational-states-v2-0";
    // private static final String NGA_STATES_V2_CENTROIDS = "GRID3-NGA-operational-states-v2-0-centroids";
    private static final String NGA_STATES_V3 = "GRID3-NGA-operational-states-v3-0";
    private static final String NGA_STATES_V3_CENTROIDS = "GRID3-NGA-operational-states-v3-0-centroids";
    // private static final String NGA_LGAS_V2 = "GRID3-NGA-operational-lgas-v2-0";
    // private static final String NGA_LGAS_V2_CENTROIDS = "GRID3-NGA-operational-lgas-v2-0-centroids";
    private static final String NGA_LGAS_V3 = "GRID3-NGA-operational-lgas-v3-0";
    private static final String NGA_LGAS_V3_CENTROIDS = "GRID3-NGA-operational-lgas-v3-0-centroids";
    // private static final String NGA_WARDS_V2 = "GRID3-NGA-operational-wards-v2-0";
    // private static final String NGA_WARDS_V2_CENTROIDS = "GRID3-NGA-operational-wards-v2-0-centroids";
    private static final String NGA_WARDS_V3 = "GRID3-NGA-operational-wards-v3-0";
    private static final String NGA_WARDS_V3_CENTROIDS = "GRID3-NGA-operational-wards-v3-0-centroids";

    // NGA has its own schema/vocabulary for health facilities (English
    // facility_level/facility_type/state_standard/lga_standard/ward_standard
    // vs. COD's French esstype/province/antenne/zonesante) — not a drop-in
    // reuse of processHealthFacilities. Flat min_zoom for now (matches COD's
    // esstype "otherwise" fallback tier) until real facility_level categories
    // are available to step by.
    private static final String NGA_HEALTH_FACILITIES = "GRID3-NGA-health-facilities-v3-0";

    // NGA settlement blocks (v4.1) mostly matches COD's v4.0 schema, but
    // renames building_area_density -> building_area_percentage and adds
    // bd_class/ma_class. Settlement extents (v4.1) is a true 1:1 schema match
    // to COD's v4.0 extents, so it reuses processSettlementExtents directly.
    private static final String NGA_SETTLEMENT_BLOCKS = "GRID3-NGA-settlement-blocks-v4-1";
    private static final String NGA_SETTLEMENT_EXTENTS = "GRID3-NGA-settlement-extents-v4-1";

    // (source name, parquet filename, per-feature handler) triples — one row
    // per GRID3 source. Single source of truth for both registerSourceHandler
    // wiring below and the addParquetSource wiring in main(), so adding a new
    // country dataset means adding one row here instead of editing two places.
    private record SourceSpec(String name, String parquetFile, FeatureProcessor handler) {}

    private final List<SourceSpec> sources;

    public GRID3() {
        sources = List.of(
                new SourceSpec("cod_province", "GRID3_COD_province_v9_0.parquet", this::processProvince),
                new SourceSpec("cod_antenne", "GRID3_COD_antenne_v9_0.parquet", this::processAntenne),
                new SourceSpec("cod_zonesante", "GRID3_COD_zonesante_v9_0.parquet", this::processZonesante),
                new SourceSpec("cod_airesante", "GRID3_COD_airesante_v9_0.parquet", this::processAiresante),
                new SourceSpec("cod_settlement_blocks", "GRID3_COD_settlement_blocks_v4_0.parquet",
                        this::processSettlementBlocks),
                new SourceSpec("cod_settlement_extents", "GRID3_COD_settlement_extents_v4_0.parquet",
                        this::processSettlementExtents),
                new SourceSpec("cod_health_facilities", "GRID3_COD_health_facilities_v9_0.parquet",
                        this::processHealthFacilities),
                new SourceSpec("cod_settlement_names", "GRID3_COD_settlement_names_v9_0.parquet",
                        this::processSettlementNames),
                new SourceSpec("cod_roads", "GRID3_COD_roads_v1_0.parquet", this::processCODRoads),
                new SourceSpec("nga_roads", "GRID3_NGA_roads_v1_0.parquet", this::processNGARoads),
                new SourceSpec("af_settlement_extents", "GRID3_AF_settlement_extents_v3_0.parquet",
                        this::processAfricaSettlementExtents),
                // new SourceSpec("nga_operational_states_v2", "GRID3_NGA_operational_states_v2_0.parquet",
                //         (source, features) -> processStates(NGA_STATES_V2, NGA_STATES_V2_CENTROIDS, source,
                //                 features)),
                new SourceSpec("nga_operational_states_v3", "GRID3_NGA_operational_states_v3_0.parquet",
                        (source, features) -> processStates(NGA_STATES_V3, NGA_STATES_V3_CENTROIDS, source,
                                features)),
                // new SourceSpec("nga_operational_lgas_v2", "GRID3_NGA_operational_lgas_v2_0.parquet",
                //         (source, features) -> processLGAs(NGA_LGAS_V2, NGA_LGAS_V2_CENTROIDS, source, features)),
                new SourceSpec("nga_operational_lgas_v3", "GRID3_NGA_operational_lgas_v3_0.parquet",
                        (source, features) -> processLGAs(NGA_LGAS_V3, NGA_LGAS_V3_CENTROIDS, source, features)),
                // new SourceSpec("nga_operational_wards_v2", "GRID3_NGA_operational_wards_v2_0.parquet",
                //         (source, features) -> processWards(NGA_WARDS_V2, NGA_WARDS_V2_CENTROIDS, source,
                //                 features)),
                new SourceSpec("nga_operational_wards_v3", "GRID3_NGA_operational_wards_v3_0.parquet",
                        (source, features) -> processWards(NGA_WARDS_V3, NGA_WARDS_V3_CENTROIDS, source,
                                features)),
                new SourceSpec("nga_health_facilities", "GRID3_NGA_health_facilities_v3_0.parquet",
                        this::processNGAHealthFacilities),
                new SourceSpec("nga_settlement_blocks", "GRID3_NGA_settlement_blocks_v4_1.parquet",
                        this::processNGASettlementBlocks),
                new SourceSpec("nga_settlement_extents", "GRID3_NGA_settlement_extents_v4_1.parquet",
                        (source, features) -> processSettlementExtents(NGA_SETTLEMENT_EXTENTS, source, features)));

        for (var spec : sources) {
            registerSourceHandler(spec.name(), spec.handler());
        }
        registerHandler(new RoadsPostProcessor(COD_ROADS));
        registerHandler(new RoadsPostProcessor(NGA_ROADS));
    }

    /** Exposes the source table to main() so parquet-source wiring shares it instead of duplicating it. */
    List<SourceSpec> sources() {
        return sources;
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
        copyAttrs(features.polygon(ANTENNE).setMinZoom(6), source,
                "antenne", "pays", "iso3", "province", "prov_uid", "date", "edit_par", "grid3id");

        var centroid = features.centroidIfConvex(ANTENNE_CENTROIDS).setMinZoom(6);
        copyAttrs(centroid, source, "antenne", "pays", "iso3", "province", "prov_uid", "date", "edit_par", "grid3id");
        setAreaKm2(centroid, source);
    }

    private void processZonesante(SourceFeature source, FeatureCollector features) {
        var poly = features.polygon(ZONESANTE).setMinZoom(7).setPixelTolerance(0.2);
        copyAttrs(poly, source, "pays", "iso3", "province", "prov_uid", "antenne", "zonesante", "zs_uid",
                "date", "source_acronym", "sourceid", "edit_par", "grid3id");

        var centroid = features.centroidIfConvex(ZONESANTE_CENTROIDS).setMinZoom(7);
        copyAttrs(centroid, source, "pays", "iso3", "province", "prov_uid", "antenne", "zonesante", "zs_uid",
                "date", "source_acronym", "sourceid", "edit_par", "grid3id");
        setAreaKm2(centroid, source);
    }

    private void processAiresante(SourceFeature source, FeatureCollector features) {
        var poly = features.polygon(AIRESANTE).setMinZoom(9)
                .setPixelTolerance(0.3).setPixelToleranceAtMaxZoom(0.1);
        copyAttrs(poly, source, "pays", "iso3", "province", "prov_uid", "antenne", "zonesante", "zs_uid",
                "airesante", "as_uid", "asnom_alt", "date", "source_acronym", "sourceid", "edit_par", "grid3id");

        var centroid = features.centroidIfConvex(AIRESANTE_CENTROIDS).setMinZoom(9);
        copyAttrs(centroid, source, "pays", "iso3", "province", "prov_uid", "antenne", "zonesante", "zs_uid",
                "airesante", "as_uid", "asnom_alt", "date", "source_acronym", "sourceid", "edit_par", "grid3id");
        setAreaKm2(centroid, source);
    }

    // ── Settlement extents / blocks ────────────────────────────────────────────
    // DOI: 10.7916/bb5b-9b79 (v4.0, published 2026-04-03) for both layers below.
    // See GRID3_latest.yaml for the full two-tier rationale (blocks = raw
    // per-block data z11+, extents = dissolved-by-mgrs_code overview z7+,
    // produced by preprocessing/utilities/dissolveBlocks_COD_duckdb.py).
    // Split core (always present, used for styling/identification) vs. detail
    // (gated to z13+) attrs — at 1.15M COD + 2.5M NGA block features all
    // starting from the same z11, the long tail of granular stats doesn't
    // need to duplicate into every z11-z12 overview tile.
    private static final String[] SETTLEMENT_BLOCK_CORE_ATTRS = {
        "block_id", "country", "iso3", "extent_type", "mgrs_code", "composite_class",
        "building_count", "building_count_density_quantile_rank"
    };
    private static final String[] SETTLEMENT_BLOCK_DETAIL_ATTRS = {
    };
    private static final int SETTLEMENT_BLOCK_DETAIL_MINZOOM = 13;

    private void processSettlementBlocks(SourceFeature source, FeatureCollector features) {
        // Conservative on purpose: no min-pixel-size here — dropping a whole block
        // feature would punch a visible hole in what's supposed to read as a
        // contiguous block mosaic.
        var poly = features.polygon(COD_SETTLEMENT_BLOCKS).setMinZoom(12)
                .setPixelTolerance(0.5).setPixelToleranceAtMaxZoom(0.1);
        copyAttrs(poly, source, SETTLEMENT_BLOCK_CORE_ATTRS);
        copyAttrsWithMinzoom(poly, source, SETTLEMENT_BLOCK_DETAIL_MINZOOM, SETTLEMENT_BLOCK_DETAIL_ATTRS);
    }

    private void processSettlementExtents(SourceFeature source, FeatureCollector features) {
        processSettlementExtents(COD_SETTLEMENT_EXTENTS, source, features);
    }

    // Also used for NGA_SETTLEMENT_EXTENTS (v4.1) — its schema is a true 1:1
    // match to COD's v4.0 extents, so it's processed identically, just tagged
    // to a different output layer.
    private void processSettlementExtents(String layer, SourceFeature source, FeatureCollector features) {
        // Generalized overview: more aggressive tolerance than the blocks it's
        // dissolved from, plus a min pixel size to drop slivers that are noise at
        // z7-10 scale — full detail restored at max zoom for overzooming.

        String type = source.getString("extent_type", "");
        int minZoom = switch (type) {
            case "Small Settlement Area" ->
                9;
            case "Hamlet" ->
                11;
            default ->
                7;
        };

        var poly = features.polygon(layer).setMinZoom(minZoom).setMaxZoom(12)
                .setPixelTolerance(0.5).setPixelToleranceAtMaxZoom(0.125)
                .setMinPixelSizeAtAllZooms(3).setMinPixelSizeAtMaxZoom(0);
        copyAttrs(poly, source, "mgrs_code", "country", "iso3", "extent_type", "composite_class",
                "block_area_sqm", "block_perimeter", "building_count",
                "building_area_sum", "dissolved_block_count");
    }

    // ── NGA settlement blocks (v4.1) ───────────────────────────────────────────
    // Not a drop-in reuse of processSettlementBlocks: this schema renames
    // building_area_density -> building_area_percentage and adds bd_class/
    // ma_class classification columns not present in COD's v4.0. `fid`,
    // `Shape__Area`, `Shape__Length` are dropped as index/geometry-derived
    // artifacts (same rationale as ORIG_FID in the roads layer). Same core/
    // detail zoom-gating split as COD's settlement blocks — bd_class/ma_class
    // are kept in "core" alongside composite_class since all three are
    // categorical fields plausibly used for choropleth styling.
    private static final String[] NGA_SETTLEMENT_BLOCK_CORE_ATTRS = {
        "block_id", "country", "iso3", "extent_type", "mgrs_code", "composite_class",
        "building_count", "building_count_density_quantile_rank", "bd_class", "ma_class"
    };
    private static final String[] NGA_SETTLEMENT_BLOCK_DETAIL_ATTRS = {
    };

    private void processNGASettlementBlocks(SourceFeature source, FeatureCollector features) {
        var poly = features.polygon(NGA_SETTLEMENT_BLOCKS).setMinZoom(12)
                .setPixelTolerance(0.5).setPixelToleranceAtMaxZoom(0.1);
        copyAttrs(poly, source, NGA_SETTLEMENT_BLOCK_CORE_ATTRS);
        copyAttrsWithMinzoom(poly, source, SETTLEMENT_BLOCK_DETAIL_MINZOOM, NGA_SETTLEMENT_BLOCK_DETAIL_ATTRS);
    }

    // ── NGA operational admin hierarchy ────────────────────────────────────────
    // 3 tiers (state/lga/ward) vs. COD's 4. Zoom tiers chosen to mirror COD's
    // spacing: state ~ province/antenne (z3), lga ~ zonesante (z5, pixel
    // tolerance 0.2), ward ~ airesante (z7, graded pixel tolerance). v2_0 and
    // v3_0 share an identical field schema per tier, so each is processed by
    // the same method, just tagged to its own versioned output layer — kept
    // separate rather than merged because v3_0 is missing some states present
    // in v2_0 (to be filtered later).
    private void processStates(String layer, String centroidLayer, SourceFeature source,
            FeatureCollector features) {
        copyAttrs(features.polygon(layer).setMinZoom(3), source,
                "country", "iso3", "state", "statecode", "multipart_count", "source", "date", "area_sqkm");

        var centroid = features.centroidIfConvex(centroidLayer).setMinZoom(3);
        copyAttrs(centroid, source,
                "country", "iso3", "state", "statecode", "multipart_count", "source", "date", "area_sqkm");
        setAreaKm2(centroid, source);
    }

    private void processLGAs(String layer, String centroidLayer, SourceFeature source,
            FeatureCollector features) {
        var poly = features.polygon(layer).setMinZoom(5).setPixelTolerance(0.2);
        copyAttrs(poly, source,
                "country", "iso3", "state", "statecode", "lga", "multipart_count", "source", "date", "area_sqkm");

        var centroid = features.centroidIfConvex(centroidLayer).setMinZoom(5);
        copyAttrs(centroid, source,
                "country", "iso3", "state", "statecode", "lga", "multipart_count", "source", "date", "area_sqkm");
        setAreaKm2(centroid, source);
    }

    private void processWards(String layer, String centroidLayer, SourceFeature source,
            FeatureCollector features) {
        var poly = features.polygon(layer).setMinZoom(7)
                .setPixelTolerance(0.3).setPixelToleranceAtMaxZoom(0.1);
        copyAttrs(poly, source, "country", "iso3", "state", "statecode", "lga", "lga_alt_names", "ward",
                "ward_alt_names", "multipart_count", "source",
                "date", "area_sqkm");

        var centroid = features.centroidIfConvex(centroidLayer).setMinZoom(7);
        copyAttrs(centroid, source, "country", "iso3", "state", "statecode", "lga", "lga_alt_names", "ward",
                "ward_alt_names", "multipart_count", "source",
                "date", "area_sqkm");
        setAreaKm2(centroid, source);
    }

    // ── NGA health facilities (v3.0) ───────────────────────────────────────────
    // Distinct schema from COD's health facilities — English facility_level/
    // facility_type/state_standard/lga_standard/ward_standard vocabulary, not
    // COD's French esstype/province/antenne/zonesante. Flat min_zoom for now
    // (matches COD's esstype "otherwise" fallback tier) — revisit with a
    // switch on facility_level once real category values are confirmed.
    // `latitude`/`longitude` dropped as redundant with point geometry (same
    // as COD's lat/lon/precision_); OBJECTID/flag*/issues/input_data_*/
    // __index_level_0__ dropped as row-index or QA-workflow artifacts not
    // meant for the public tileset.
    private void processNGAHealthFacilities(SourceFeature source, FeatureCollector features) {
        var point = features.point(NGA_HEALTH_FACILITIES).setMinZoom(10);
        copyAttrs(point, source, "unique_id", "country", "iso", "state_standard", "lga_standard", "ward_standard",
                "facility_name", "alt_name", "settlement_name", "facility_level", "facility_type",
                "facility_ownership", "facility_ownership_type", "functional", "date_created", "sett_ext_type",
                "mgrs_code", "nhfr_facility_code");
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
            case "Small Settlement Area" ->
                9;
            case "Hamlet" ->
                12;
            default ->
                7;
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
            case "Bureau Central de la Zone de Santé", "Hôpital Général de Référence" ->
                8;
            case "Hôpital", "Centre Hopitalier" ->
                9;
            case "Centre Médical", "Centre de Santé de Référence", "Centre de Santé", "Poste de Santé" ->
                10;
            case "Clinique", "Polyclinque", "Dispensaire", "Site Soin Communautaire", "Autre" ->
                13;
            default ->
                10;
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
    private void processCODRoads(SourceFeature source, FeatureCollector features) {
        processRoads(COD_ROADS, source, features);
    }

    private void processNGARoads(SourceFeature source, FeatureCollector features) {
        processRoads(NGA_ROADS, source, features);
    }

    private void processRoads(String layer, SourceFeature source, FeatureCollector features) {
        String cls = source.getString("class", "");
        int minZoom = switch (cls) {
            case "service", "steps" ->
                13;
            case "tertiary" ->
                10;
            case "secondary" ->
                7;
            case "primary" ->
                6;
            case "motorway", "trunk" ->
                5;
            default ->
                11;
        };

        var line = features.line(layer).setMinZoom(minZoom);
        copyAttrs(line, source, "grid3_id", "country", "iso3", "class", "subclass",
                "names", "label", "road_surface", "speed_estimate", "speed_limits");
        // Provenance/QA metadata, not used for MapLibre styling — gated to z11+
        // so it doesn't duplicate into every tile of the wide-area motorway/
        // trunk/primary/secondary classes, which render from as low as z5.
        copyAttrsWithMinzoom(line, source, 11, "gers", "source_id", "speed_estimate_method", "date",
                "source_acronym");
    }


    /**
     * Fuses touching same-attribute road segments (this dataset carries
     * Overture GERS-style over-segmentation) and drops merged segments under ~2
     * tile pixels below max zoom, keeping full detail at max zoom for
     * overzooming — same min_length/tolerance/buffer values as
     * tile_post_process.merge_line_strings in GRID3_latest.yaml.
     */
    private static final class RoadsPostProcessor implements LayerPostProcessor {

        private final String layer;

        RoadsPostProcessor(String layer) {
            this.layer = layer;
        }

        @Override
        public String name() {
            return layer;
        }

        @Override
        public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) throws GeometryException {
            double minLength = zoom >= MAXZOOM ? 0 : 2;
            double tolerance = zoom >= MAXZOOM ? 0.1 : 0.5;
            return FeatureMerge.mergeLineStrings(items, minLength, tolerance, 4, true);
        }
    }

    
    // ── Helpers ─────────────────────────────────────────────────────────────────
    private static void copyAttrs(FeatureCollector.Feature feature, SourceFeature source, String... keys) {
        for (String key : keys) {
            feature.setAttr(key, source.getTag(key));
        }
    }

    /**
     * Like {@link #copyAttrs}, but the attributes only appear in tiles from
     * {@code minzoom} up — for provenance/analytical fields that aren't used
     * for MapLibre styling and don't need to duplicate into every low-zoom
     * tile of a layer that spans a huge area at its own min zoom (e.g.
     * motorway-class roads, which render from z5).
     */
    private static void copyAttrsWithMinzoom(FeatureCollector.Feature feature, SourceFeature source, int minzoom,
            String... keys) {
        for (String key : keys) {
            feature.setAttrWithMinzoom(key, source.getTag(key), minzoom);
        }
    }

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
        return "GRID3 LATEST";
    }

    @Override
    public String description() {
        return "GRID3 latest derived vector tiles. Source data available for download at https://data.grid3.org.";
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
    // hive partitioning, unlike Overture's theme=/type= layout). Wires
    // addParquetSource(name, List.of(path)) once per row of the SourceSpec
    // table above, instead of a hand-duplicated call per source.
    private static final int MAXZOOM = 15;

    public static void main(String[] args) throws Exception {
        var arguments = Arguments.fromArgsOrConfigFile(args).orElse(Arguments.of("maxzoom", MAXZOOM));
        String dataDir = arguments.getString("data_dir",
                "Directory of GRID3 GeoParquet source data",
                "/tmp/grid3_tiles/data/2-scratch/grid3/latest");
        Path dir = Path.of(dataDir);

        var profile = new GRID3();
        var planetiler = Planetiler.create(arguments).setProfile(profile);
        for (var spec : profile.sources()) {
            planetiler.addParquetSource(spec.name(), List.of(dir.resolve(spec.parquetFile())));
        }
        planetiler.overwriteOutput(Path.of("latest.pmtiles")).run();
    }
}
