/**
 * Source manifest — all PMTiles archives served by this app.
 *
 * Each key is the MapLibre source ID used in style.json.
 * archive: R2 object path, without .pmtiles extension.
 * ext:     tile format served by the Cloudflare Worker.
 *
 * The Cloudflare Worker resolves:
 *   {WORKER_URL}/{archive}/{z}/{x}/{y}.{ext}  →  {archive}.pmtiles in R2
 *
 * To add a new layer:
 *   1. Add an entry here.
 *   2. Add a matching source stub in style.json (type + empty tiles: []).
 *   3. Add layers in style.json that reference this source.
 *   No other files need to change.
 */

export interface SourceDef {
  archive: string;
  attribution: string;
  maxzoom: number;
  ext: "mvt" | "webp";
}

export const SOURCES = {

  // ---------------------------------------------------------------------------
  // Base layers
  // ---------------------------------------------------------------------------

  protomaps_base: {
    archive: "tiles/protomaps/africa_base",
    attribution: '<a href="https://github.com/protomaps/basemaps">Protomaps</a> © <a href="https://openstreetmap.org">OpenStreetMap Contributors</a>',
    maxzoom: 15,
    ext: "mvt",
  },

  protomaps_bg: {
    archive: "tiles/protomaps/base_z4",
    attribution: '<a href="https://github.com/protomaps/basemaps">Protomaps</a> © <a href="https://openstreetmap.org">OpenStreetMap Contributors</a>',
    maxzoom: 4,
    ext: "mvt",
  },

  overture_base: {
    archive: "tiles/overture/base",
    attribution: '<a href="https://overturemaps.org">Overture Maps Foundation</a>',
    maxzoom: 13,
    ext: "mvt",
  },

  overture_buildings: {
    archive: "tiles/overture/buildings_composite",
    attribution: '<a href="https://overturemaps.org">Overture Maps Foundation</a>',
    maxzoom: 14,
    ext: "mvt",
  },


  terrain: {
    archive: "tiles/mapterhorn/terrain",
    attribution: '<a href="https://mapterhorn.com/attribution">© Mapterhorn</a>',
    maxzoom: 12,
    ext: "webp",
  },


  // ---------------------------------------------------------------------------
  // GRID3 composite
  // ---------------------------------------------------------------------------

    grid3: {
    archive: "tiles/grid3/africa/GRID3_africa",
    attribution: '<a href="https://ciesin.columbia.edu/">© CIESIN Columbia University</a>',
    maxzoom: 14,
    ext: "mvt",
  },


  // ---------------------------------------------------------------------------
  // GRID3 ISO3 consolidated
  // ---------------------------------------------------------------------------

  grid3_cod: {
    archive: "tiles/grid3/cod/GRID3_COD",
    attribution: '<a href="https://ciesin.columbia.edu/">© CIESIN Columbia University</a>',
    maxzoom: 14,
    ext: "mvt",
  },

  grid3_nga: {
    archive: "tiles/grid3/nga/GRID3_NGA",
    attribution: '<a href="https://ciesin.columbia.edu/">© CIESIN Columbia University</a>',
    maxzoom: 15,
    ext: "mvt",
  },



  // ---------------------------------------------------------------------------
  // GRID3 — COD (Democratic Republic of Congo)
  // ---------------------------------------------------------------------------


  grid3_cod_roads: {
    archive: "tiles/grid3/cod/GRID3_COD_roads",
    attribution: '<a href="https://ciesin.columbia.edu/">© CIESIN Columbia University</a>',
    maxzoom: 14,
    ext: "mvt",
  },


  grid3_cod_boundaries: {
    archive: "tiles/grid3/cod/GRID3_COD_boundaries",
    attribution: '<a href="https://ciesin.columbia.edu/">© CIESIN Columbia University</a>',
    maxzoom: 14,
    ext: "mvt",
  },


  grid3_cod_pois: {
    archive: "tiles/grid3/cod/GRID3_COD_POIs",
    attribution: '<a href="https://ciesin.columbia.edu/">© CIESIN Columbia University</a>',
    maxzoom: 16,
    ext: "mvt",
  },

  grid3_cod_settlement_extents: {
    archive: "tiles/grid3/cod/GRID3_COD_settlement_extents",
    attribution: '<a href="https://ciesin.columbia.edu/">© CIESIN Columbia University</a>',
    maxzoom: 15,
    ext: "mvt",
  },

  // ---------------------------------------------------------------------------
  // GRID3 — NGA (Nigeria)
  // ---------------------------------------------------------------------------

  grid3_nga_settlement_extents: {
    archive: "tiles/grid3/nga/GRID3_NGA_settlement_extents",
    attribution: '<a href="https://ciesin.columbia.edu/">© CIESIN Columbia University</a>',
    maxzoom: 15,
    ext: "mvt",
  },

    grid3_nga_roads: {
    archive: "tiles/grid3/nga/GRID3_NGA_roads",
    attribution: '<a href="https://ciesin.columbia.edu/">© CIESIN Columbia University</a>',
    maxzoom: 14,
    ext: "mvt",
  },


  grid3_nga_boundaries: {
    archive: "tiles/grid3/nga/GRID3_NGA_boundaries",
    attribution: '<a href="https://ciesin.columbia.edu/">© CIESIN Columbia University</a>',
    maxzoom: 15,
    ext: "mvt",
  },

  // ---------------------------------------------------------------------------
  // GRID3 — Settlement extents by country (ISO 3166-1 alpha-3)
  // Add new countries below. Uncomment and set the correct version when data
  // is available in R2.
  // ---------------------------------------------------------------------------

  // grid3_dji_settlements: {
  //   archive: "tiles/grid3/dji/settlement_extents_v3_0",
  //   attribution: '<a href="https://ciesin.columbia.edu/">© CIESIN Columbia University</a>',
  //   maxzoom: 14,
  //   ext: "mvt",
  // },

  // grid3_eri_settlements: {
  //   archive: "tiles/grid3/eri/settlement_extents_v3_0",
  //   attribution: '<a href="https://ciesin.columbia.edu/">© CIESIN Columbia University</a>',
  //   maxzoom: 14,
  //   ext: "mvt",
  // },

  // grid3_eth_settlements: {
  //   archive: "tiles/grid3/eth/settlement_extents_v3_0",
  //   attribution: '<a href="https://ciesin.columbia.edu/">© CIESIN Columbia University</a>',
  //   maxzoom: 14,
  //   ext: "mvt",
  // },

  // grid3_gab_settlements: {
  //   archive: "tiles/grid3/gab/settlement_extents_v3_0",
  //   attribution: '<a href="https://ciesin.columbia.edu/">© CIESIN Columbia University</a>',
  //   maxzoom: 14,
  //   ext: "mvt",
  // },

  // grid3_gha_settlements: {
  //   archive: "tiles/grid3/gha/settlement_extents_v3_0",
  //   attribution: '<a href="https://ciesin.columbia.edu/">© CIESIN Columbia University</a>',
  //   maxzoom: 14,
  //   ext: "mvt",
  // },

} satisfies Record<string, SourceDef>;

export type SourceKey = keyof typeof SOURCES;

// ---------------------------------------------------------------------------
// Static assets — sprites and fonts served from R2 via the Worker.
//
// The Worker serves any request matching /tiles/assets/** directly from R2.
// Sprite base URLs omit the extension — MapLibre appends .json/.png/@2x.json/@2x.png.
// Font pattern uses {fontstack} and {range} placeholders (MapLibre convention).
//
// To add a new spritesheet:
//   1. Upload <name>.{json,png,@2x.json,@2x.png} to R2 at the path below.
//   2. Add an entry here.
//   3. Reference it in config.ts and inject via getMaplibreStyle() in MapView.tsx.
// ---------------------------------------------------------------------------

export interface SpriteDef {
  /** R2 key prefix — Worker serves {path}.{json,png,@2x.json,@2x.png} */
  path: string;
}

export interface FontDef {
  /** R2 key prefix — Worker serves {path}/{fontstack}/{range}.pbf */
  path: string;
}

export const ASSETS = {
  sprites: {
    maki: {
      path: "tiles/assets/icons/maki",
    } satisfies SpriteDef,
  },

  fonts: {
    // Placeholder — populate tiles/assets/fonts/ in R2 before switching from CDN
    // noto: {
    //   path: "tiles/assets/fonts",
    // } satisfies FontDef,
  },
} as const;
