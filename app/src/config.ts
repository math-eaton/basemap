import { ASSETS, SOURCES, type SourceKey } from "./sources";

// VITE_CLOUDFLARE_WORKER_URL is the only environment variable needed.
// It points to the Cloudflare Worker that serves tiles from R2:
//   dev:  https://dev-tileworker.ciesin.app  → ciesin-dev bucket
//   prod: https://prod-tileworker.ciesin.app → ciesin-prod bucket
//
// Set this in your .env.development / .env.production locally, or in the
// Cloudflare Pages dashboard (Settings → Environment Variables) for CI builds.
// All archive paths live in sources.ts — no other env vars are needed.

// import.meta.env is a Vite-ism, statically replaced in the app bundle but
// absent when this module is imported from a plain Node context (the
// build-style.ts script) -- optional-chain it so both work.
const env = (import.meta as { env?: Record<string, string | undefined> }).env;

const workerUrl: string =
  env?.VITE_CLOUDFLARE_WORKER_URL || "https://dev-tileworker.ciesin.app";

export const APP_CONFIG = {
  workerUrl,
  assets: {
    // Protomaps base sprites (road shields, POI markers used by the base style)
    // Served from CDN until a self-hosted flavor is built and uploaded to R2.
    protomapsSpritesUrl:
      env?.VITE_PROTOMAPS_SPRITE_URL ||
      "https://protomaps.github.io/basemaps-assets/sprites/v4/light",
    // Maki icon set — served from R2 via the Worker
    makiSpriteUrl:
      env?.VITE_MAKI_SPRITE_URL || `${workerUrl}/${ASSETS.sprites.maki.path}`,
    // Fonts — CDN until tiles/assets/fonts/ is populated in R2
    glyphsUrl:
      env?.VITE_GLYPHS_URL ||
      "https://protomaps.github.io/basemaps-assets/fonts/{fontstack}/{range}.pbf",
  },
};

/**
 * Returns the MapLibre source config for a given source key.
 * The tile URL is constructed as: {workerUrl}/{archive}/{z}/{x}/{y}.{ext}
 * The worker resolves {archive}.pmtiles from R2.
 */
export function getTileSourceConfig(key: SourceKey): {
  tiles: string[];
  attribution: string;
  maxzoom: number;
} {
  const source = SOURCES[key];
  return {
    tiles: [`${workerUrl}/${source.archive}/{z}/{x}/{y}.${source.ext}`],
    attribution: source.attribution,
    maxzoom: source.maxzoom,
  };
}

export function logConfig(): void {
  console.log("Worker URL:", workerUrl);
  for (const key of Object.keys(SOURCES) as SourceKey[]) {
    const { tiles, maxzoom } = getTileSourceConfig(key);
    console.log(`  ${key}: ${tiles[0]} (maxzoom ${maxzoom})`);
  }
}
