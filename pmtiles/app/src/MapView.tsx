/* @refresh reload */
import { render } from "solid-js/web";
import "./index.css";
import MaplibreInspect from "@maplibre/maplibre-gl-inspect";
import "@maplibre/maplibre-gl-inspect/dist/maplibre-gl-inspect.css";
import { MaplibreLegendControl } from "@watergis/maplibre-gl-legend";
import "@watergis/maplibre-gl-legend/dist/maplibre-gl-legend.css";
import * as maplibregl from "maplibre-gl";
import {
  AttributionControl,
  // GlobeControl,
  Map as MaplibreMap,
  NavigationControl,
  FullscreenControl,
  Popup,
  ScaleControl,
  getRTLTextPluginStatus,
  setRTLTextPlugin,
  type IControl,
} from "maplibre-gl";
import type {
  LngLatBoundsLike,
  // MapGeoJSONFeature,
  // MapTouchEvent,
  StyleSpecification,
} from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
// import type { LayerSpecification } from "@maplibre/maplibre-gl-style-spec";
import {
  // For,
  createSignal,
  onMount,
} from "solid-js";
import { getTileSourceConfig, APP_CONFIG } from "./config";
import { SOURCES, type SourceKey } from "./sources";
import baseStyle from "./style.json";

// Light configuration for 3D features
// const LIGHT_CONFIG = {
//   anchor: "map" as const, // 'viewport' or 'map'
//   position: [240, 15, 45] as [number, number, number], // [radial, azimuthal, polar] in degrees
//   color: "#ffffff",
//   intensity: 0.15, // 0 to 1
// };

// function getSourceLayer(l: LayerSpecification): string {
//   if ("source-layer" in l && l["source-layer"]) {
//     return l["source-layer"];
//   }
//   return "";
// }

// Custom control for attribution info button
class AttributionInfoControl implements IControl {
  private _container?: HTMLDivElement;
  private _button?: HTMLButtonElement;

  onAdd(_map: MaplibreMap): HTMLElement {
    this._container = document.createElement('div');
    this._container.className = 'maplibregl-ctrl maplibregl-ctrl-group';
    
    this._button = document.createElement('button');
    this._button.className = 'maplibregl-ctrl-icon';
    this._button.type = 'button';
    this._button.title = 'View full attribution and data sources';
    this._button.innerHTML = '?';
    this._button.style.fontSize = '18px';
    this._button.style.fontWeight = 'bold';
    this._button.style.cursor = 'pointer';
    
    this._button.addEventListener('click', () => {
      window.open('/attribution.html', '_blank');
    });
    
    this._container.appendChild(this._button);
    return this._container;
  }

  onRemove(): void {
    if (this._container && this._container.parentNode) {
      this._container.parentNode.removeChild(this._container);
    }
  }
}

// const featureIdToOsmId = (raw: string | number) => {
//   return Number(BigInt(raw) & ((BigInt(1) << BigInt(44)) - BigInt(1)));
// };

// const featureIdToOsmType = (i: string | number) => {
//   const t = (BigInt(i) >> BigInt(44)) & BigInt(3);
//   if (t === BigInt(1)) return "node";
//   if (t === BigInt(2)) return "way";
//   if (t === BigInt(3)) return "relation";
//   return "not_osm";
// };

// const displayId = (featureId?: string | number) => {
//   if (featureId) {
//     const osmType = featureIdToOsmType(featureId);
//     if (osmType !== "not_osm") {
//       const osmId = featureIdToOsmId(featureId);
//       return (
//         <a
//           class="underline text-purple"
//           target="_blank"
//           rel="noreferrer"
//           href={`https://openstreetmap.org/${osmType}/${osmId}`}
//         >
//           {osmType} {osmId}
//         </a>
//       );
//     }
//   }
//   return featureId;
// };

// const FeaturesProperties = (props: { features: MapGeoJSONFeature[] }) => {
//   return (
//     <div class="features-properties">
//       <For each={props.features}>
//         {(f) => (
//           <div>
//             <span>
//               <strong>{getSourceLayer(f.layer)}</strong>
//               <span> ({f.geometry.type})</span>
//             </span>
//             <table>
//               <tbody>
//                 <tr>
//                   <td>id</td>
//                   <td>{displayId(f.id)}</td>
//                 </tr>
//                 <For each={Object.entries(f.properties)}>
//                   {([key, value]) => (
//                     <tr>
//                       <td>{key}</td>
//                       <td>{value}</td>
//                     </tr>
//                   )}
//                 </For>
//               </tbody>
//             </table>
//           </div>
//         )}
//       </For>
//     </div>
//   );
// };

function getMaplibreStyle(demSource: any): StyleSpecification {
  const style = JSON.parse(JSON.stringify(baseStyle)) as StyleSpecification;

  // Inject tile URLs for every source defined in sources.ts whose key appears
  // in style.json. Adding a new country: add to sources.ts + style.json only.
  for (const key of Object.keys(SOURCES) as SourceKey[]) {
    if (key in style.sources) {
      const cfg = getTileSourceConfig(key);
      style.sources[key] = {
        type: "vector",
        attribution: cfg.attribution,
        tiles: cfg.tiles,
        maxzoom: cfg.maxzoom,
      };
    }
  }

  // DEM and contours are generated at runtime by maplibre-contour, not served
  // from R2 archives, so they are handled separately below.
  const terrainMaxzoom = SOURCES.terrain.maxzoom;
  style.sources.dem = {
    type: "raster-dem",
    encoding: "terrarium",
    tiles: [demSource.sharedDemProtocolUrl],
    maxzoom: terrainMaxzoom,
    tileSize: 256,
  };

  // contour steps in meters (each pair is [minor, major] contour intervals for that zoom level)
  // nb: mapterhorn dem source is high res, so very small contour invervals
  // will run into maplibre-gl vertex limits
style.sources.contours = {
  type: "vector",
  tiles: [
    demSource.contourProtocolUrl({
      multiplier: 1, // meters
      thresholds: {
        10.5: [60, 300],

        11.5: [50, 250],

        12.5: [20, 100],

        13.5: [10, 50],

        14.5: [10, 50],

        15.5: [5, 25],
      },
      elevationKey: "ele",
      levelKey: "level",
      contourLayer: "contours",
    }),
  ],
  minzoom: 10,
};

  // Multi-sprite: "default" keeps protomaps base icons; "maki" adds our icon set.
  // Reference maki icons in style layers as "maki:hospital", "maki:school", etc.
  style.sprite = [
    { id: "default", url: APP_CONFIG.assets.protomapsSpritesUrl },
    { id: "maki",    url: APP_CONFIG.assets.makiSpriteUrl },
  ];
  style.glyphs = APP_CONFIG.assets.glyphsUrl;

  return style;
}

const LEGEND_LAYERS: Record<string, string> = {
  // "cod-health-facilities":     "Établissement de santé"
  "nga-settlement-extents":     "Settlement extent",
  // "nga-settlement-blocks":      "Settlement block",
  "cod-provinces-boundary":    "Limite de la province",
  "cod-antenne-boundary":      "Limite de l'antenne",
  "cod-health-zones-boundary": "Limite de la zone de santé",
  "cod-health-areas-boundary": "Limite de l'aire de santé",
  "cod-settlement-extents":     "Zone de bâtiments",
  // "cod-settlement-blocks":      "Pâté de maisons"
};

function MapLibreView() {
  let mapContainer: HTMLDivElement | undefined;
  let hiddenRef: HTMLDivElement | undefined;
  // let longPressTimeout: ReturnType<typeof setTimeout>;

  const [zoom, setZoom] = createSignal<number>(0);

  onMount(async () => {
    // Log tile configuration for debugging
    // logConfig();

    if (getRTLTextPluginStatus() === "unavailable") {
      setRTLTextPlugin(
        "https://unpkg.com/@mapbox/mapbox-gl-rtl-text@0.2.3/mapbox-gl-rtl-text.min.js",
        true,
      );
    }

    if (!mapContainer) {
      console.error("Could not mount map element");
      return;
    }

    // Using Cloudflare Worker for tile delivery - no PMTiles protocol needed

    // Setup maplibre-contour
    const mlcontourModule = await import("maplibre-contour");
    const mlcontour = mlcontourModule.default;
    
    // Get terrain tile URL from Cloudflare Worker
    const terrainConfig = getTileSourceConfig("terrain");
    const terrainTileUrl = terrainConfig.tiles[0];
    
    // Create DEM source using Mapterhorn terrain tiles from R2
    const demSource = new mlcontour.DemSource({
      url: terrainTileUrl,
      encoding: "terrarium",
      maxzoom: terrainConfig.maxzoom || 12,
      worker: true,
      cacheSize: 100,
      timeoutMs: 10_000,
    });
    
    // Setup maplibre with the DemSource
    demSource.setupMaplibre(maplibregl);

    // const drcBounds: LngLatBoundsLike = [[8, -13], [35, 9]];
    // Updated extent: WEST, SOUTH, EAST, NORTH
    const africaBounds: LngLatBoundsLike = [[-19.1, -36.1], [52.6, 38.4]];
    // const mapBounds: LngLatBoundsLike = [[-50, -50], [70, 70]]; // wider bounds to avoid tile clipping at edges

    // Get style with contours
    const style = getMaplibreStyle(demSource);

    const map = new MaplibreMap({
      hash: "map",
      container: mapContainer,
      style: style,
      center: [14.08, 0],
      zoom: 2, 
      minZoom: 3,
      maxZoom: 18,
      maxBounds: africaBounds, // viewport restriction
      attributionControl: false,
      refreshExpiredTiles: false,
      maxTileCacheSize: 500,
      // cancelPendingTileRequestsWhileZooming: true,
      renderWorldCopies: false,
      fadeDuration: 200
    });

    map.addControl(new NavigationControl());
    // map.addControl(new GlobeControl());
    map.addControl(new AttributionInfoControl());

    // Add scale control at bottom-left
    map.addControl(new ScaleControl(
      {maxWidth: 200, unit: 'metric'}
    ), 'bottom-left');

    map.addControl(
      new AttributionControl({
        compact: false,
      }),
    );

    // Make the entire attribution control clickable
    setTimeout(() => {
      const attrControl = document.querySelector('.maplibregl-ctrl-attrib');
      if (attrControl) {
        attrControl.setAttribute('title', 'Click for full attribution and data sources');
        (attrControl as HTMLElement).style.cursor = 'pointer';
        attrControl.addEventListener('click', (e) => {
          // Only open if clicking the container, not existing links
          if ((e.target as HTMLElement).tagName !== 'A') {
            window.open('/attribution.html', '_blank');
          }
        });
      }
    }, 100);

    
    // showInspectMapPopupOnHover: false — click/tap only, safe on mobile
    // useInspectStyle: false — keep original style.json; hue-rotate signals inspect mode visually
    map.addControl(
      new MaplibreInspect({
        popup: new Popup({
          closeButton: false,
          closeOnClick: false,
        }),
        showInspectMapPopupOnHover: false,
        useInspectStyle: false,
        toggleCallback: (active) => {
          map.getCanvas().style.filter = active ? 'hue-rotate(180deg) invert(1) brightness(1.75) contrast(1.25)' : '';
        },
      }),
    );

    map.addControl(new FullscreenControl());


    // const popup = new Popup({
    //   closeButton: true,
    //   closeOnClick: true,
    //   maxWidth: "none",
    // });

    map.on("load", () => {
      map.resize();
      map.addControl(
        new MaplibreLegendControl(LEGEND_LAYERS, {
          showDefault:  true,
          showCheckbox: true,
          onlyRendered: true,
          reverseOrder: true,
        }) as unknown as IControl,
        "bottom-right",
      );

      // onlyRendered:true re-checks queryRenderedFeatures on moveend/styledata.
      // When a layer is re-enabled, styledata fires before tiles finish loading,
      // so the item disappears. Wait for idle (tiles loaded) then fire moveend.
      map.getContainer().addEventListener('change', (e: Event) => {
        const cb = e.target as HTMLInputElement;
        if (cb.type === 'checkbox' && cb.checked && cb.closest('.maplibregl-legend-list')) {
          map.once('idle', () => map.fire('moveend'));
        }
      });
    });

    map.on("error", (e) => {
      console.error("Map error:", e);
    });

    map.on("idle", () => {
      setZoom(map.getZoom());
    });

    // const showContextMenu = (e: MapTouchEvent) => {
    //   const features = map.queryRenderedFeatures(e.point);
    //   if (hiddenRef && features.length) {
    //     hiddenRef.innerHTML = "";
    //     render(() => <FeaturesProperties features={features} />, hiddenRef);
    //     popup.setHTML(hiddenRef.innerHTML);
    //     popup.setLngLat(e.lngLat);
    //     popup.addTo(map);
    //   } else {
    //     popup.remove();
    //   }
    // };

    // map.on("contextmenu", (e: MapTouchEvent) => {
    //   showContextMenu(e);
    // });

    // map.on("touchstart", (e: MapTouchEvent) => {
    //   longPressTimeout = setTimeout(() => {
    //     showContextMenu(e);
    //   }, 500);
    // });

    // const clearLongPress = () => {
    //   clearTimeout(longPressTimeout);
    // };

    map.on("zoom", (e) => {
      setZoom(e.target.getZoom());
    });

    // map.on("touchend", clearLongPress);
    // map.on("touchcancel", clearLongPress);
    // map.on("touchmove", clearLongPress);
    // map.on("pointerdrag", clearLongPress);
    // map.on("pointermove", clearLongPress);
    // map.on("moveend", clearLongPress);
    // map.on("gesturestart", clearLongPress);
    // map.on("gesturechange", clearLongPress);
    // map.on("gestureend", clearLongPress);

    return () => {
      map.remove();
    };
  });

  return (
    <>
      <div class="hidden" ref={hiddenRef} />
      <div ref={mapContainer} class="h-full w-full flex" />
      <div class="absolute top-0 left-0 p-1 text-xs bg-white bg-opacity-50">
        z@{zoom().toFixed(2)}
      </div>
    </>
  );
}

function MapView() {
  return (
    <div class="flex flex-col h-dvh w-full">
      <div class="h-full flex grow">
        <MapLibreView />
      </div>
    </div>
  );
}

const root = document.getElementById("root");

if (root) {
  render(() => <MapView />, root);
}
