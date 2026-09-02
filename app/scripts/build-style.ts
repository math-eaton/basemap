#!/usr/bin/env tsx
/**
 * Generates src/generated/style.json: a fully self-contained, Maputnik-droppable
 * MapLibre style. All layer content/ordering comes from the vendored
 * ../../styles package's grid3Layers() (../../styles/src/grid3/compose.ts) --
 * this script's only job is plugging in real tile URLs/attribution/sprite
 * from sources.ts/config.ts.
 *
 * dem/contours/hillshade are deliberately NOT part of this output -- they're
 * added at runtime by MapView.tsx via maplibre-contour, which registers a
 * custom (non-http) protocol that no other MapLibre consumer (Maputnik
 * included) can resolve. See src/terrain-layers.ts.
 */
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { StyleSpecification } from "@maplibre/maplibre-gl-style-spec";
import { grid3Layers } from "../../pmtiles/styles/src/grid3/compose.ts";
import { GRID3_LIGHT } from "../../pmtiles/styles/src/grid3/flavor";
import { SOURCES, ASSETS, type SourceKey } from "../src/sources";
import { APP_CONFIG } from "../src/config";

const __dirname = dirname(fileURLToPath(import.meta.url));

// This generated style always points at prod -- dev/staging keep building
// their own via VITE_CLOUDFLARE_WORKER_URL at runtime for the app itself.
const WORKER_ORIGIN = "https://prod-tileworker.ciesin.app";

function tileSource(key: SourceKey) {
  const s = SOURCES[key];
  return {
    type: "vector" as const,
    attribution: s.attribution,
    tiles: [`${WORKER_ORIGIN}/${s.archive}/{z}/{x}/{y}.${s.ext}`],
    maxzoom: s.maxzoom,
  };
}

const style: StyleSpecification = {
  version: 8,
  name: "GRID3",
  center: [14.08, 0],
  zoom: 2,
  sources: {
    protomaps_base: tileSource("protomaps_base"),
    protomaps_bg: tileSource("protomaps_bg"),
    overture_buildings: tileSource("overture_buildings"),
    grid3: tileSource("grid3"),
    grid3_nga: tileSource("grid3_nga"),
  },
  sprite: [
    { id: "default", url: APP_CONFIG.assets.protomapsSpritesUrl },
    // Built directly from WORKER_ORIGIN, not APP_CONFIG.assets.makiSpriteUrl --
    // that value falls back to the dev worker when no VITE_CLOUDFLARE_WORKER_URL
    // is set, which is always the case in this plain-Node build script.
    { id: "maki", url: `${WORKER_ORIGIN}/${ASSETS.sprites.maki.path}` },
  ],
  glyphs: APP_CONFIG.assets.glyphsUrl,
  layers: grid3Layers("protomaps_base", GRID3_LIGHT, { lang: "en" }),
};

const outPath = resolve(__dirname, "../src/generated/style.json");
await mkdir(dirname(outPath), { recursive: true });
await writeFile(outPath, JSON.stringify(style, null, 2));
console.log(`wrote ${outPath} (${style.layers.length} layers)`);
