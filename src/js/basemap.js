import maplibregl from 'maplibre-gl';
import { Protocol } from 'pmtiles';
import { PMTiles } from 'pmtiles';

// Lazy load contour functionality
let mlcontour = null;
let demSource = null;

async function initContours() {
    if (!mlcontour) {
        // Import the module and get the default export
        const mlcontourModule = await import('maplibre-contour');
        mlcontour = mlcontourModule.default;
        
        // Now create DemSource using the imported module
        demSource = new mlcontour.DemSource({
            url: "https://elevation-tiles-prod.s3.amazonaws.com/terrarium/{z}/{x}/{y}.png",
            encoding: "terrarium", // "mapbox" or "terrarium" default="terrarium"
            maxzoom: 13,
            worker: true, // offload isoline computation to a web worker to reduce jank
            cacheSize: 100, // number of most-recent tiles to cache
            timeoutMs: 10_000, // timeout on fetch requests
        });
        demSource.setupMaplibre(maplibregl);
    }
    return { mlcontour, demSource };
}

class OvertureMap {
    constructor(containerId, options = {}) {
        this.containerId = containerId;
        this.options = {
            // Default fallback bounds (DRC area) - will be replaced by PMTiles bounds
            bounds: [
                [22.0, -6.0], // Southwest coordinates [lng, lat]
                [24.0, -4.0]  // Northeast coordinates [lng, lat]
            ],
            // center: [-74.986763650502, 44.66997929549087],
            center: [23.5967, -6.1307],
            zoom: 14,
            minZoom: 11,
            maxZoom: 15,
            showTileBoundaries: false,
            clampToBounds: false,
            useVectorTiles: false, // Set to true to use traditional vector tiles instead of PMTiles
            useCustomCenter: true, // If true, use custom center. If false, auto-center to PMTiles extent
            ...options
        };
        
        this.map = null;
        this.protocol = null;
        
        // Layer draw order index - lower numbers draw first (bottom), higher numbers draw on top
        this.layerDrawOrder = {
            // Base layers (0-9)
            'background': 1,


            // Land use and land cover (20-39)
            'land': 2,           // Natural land features (forest, grass, etc.)
            'land-cover': 20,     // Land cover data (forest, crop, grass, etc.)

            'settlement-extents-fill': 25, // Settlement extent fills
            'settlement-extents-outlines': 89, // Settlement extent outlines

            'land-use': 30, // built env
            'land-residential': 15, // residential areas
                                    
            // Terrain and elevation 
            'hills': 47,

            // Water features (40-49)
            'water-polygons': 40,        // Water body fills
            'water-polygon-outlines': 41, // Water body outlines
            'water-lines': 42,           // Rivers, streams, canals

            // Contour lines (50-59)
            'contours': 50,       // Contour lines
            'contour-text': 51,   // Contour elevation labels

            
            // Transportation (60-79)
            'roads-solid': 60,    // Major road lines (solid)
            'roads-dashed': 61,   // Minor road lines (dashed)
            'roads-solid-background': 59, // Background for solid roads (for better contrast)

            // Infrastructure (70-79)
            'infrastructure-polygons': 70,  // Infrastructure polygon fills
            'infrastructure-lines': 71,     // Infrastructure lines (power, communication, etc.)
            'infrastructure-points': 72,    // Infrastructure points (towers, utilities, etc.)
            
            // Buildings and structures (80-89)
            'buildings-low-lod': 82,   // Building fills (low detail)
            'buildings-medium-lod': 81, // Building fills (medium detail)
            'buildings-high-lod': 80,  // Building fills (high detail)
            'building-outlines': 83,   // Building outlines

            // Points of interest (90-99)
            'places': 90,         // Place points/circles
            
            // Labels and text (100+)
            'place-labels': 100   // Place name labels - always on top
        };
        
        this.init();
    }
    
    /**
     * Initialize the PMTiles protocol and create the map
     */
    init() {
        // Initialize PMTiles protocol
        this.protocol = new Protocol();
        maplibregl.addProtocol("pmtiles", this.protocol.tile);
        
        // Load the style configuration
        this.loadStyle().then(async style => {
            this.createMap(style);
            this.setupEventHandlers();
            this.addControls();
        }).catch(error => {
            console.error('Failed to load map style:', error);
            // Show user-friendly error message
            const mapContainer = document.getElementById(this.containerId);
            mapContainer.innerHTML = '<div style="display: flex; align-items: center; justify-content: center; height: 100%; font-family: sans-serif; color: #e74c3c; text-align: center; padding: 20px;"><div><h3>Map Loading Error</h3><p>Unable to load map tiles. This may be due to hosting limitations.<br>Please try refreshing the page or contact the administrator.</p></div></div>';
        });
    }
    
    /**
     * Load the MapLibre style from JSON file
     */
    async loadStyle() {
        try {
            // Choose style file based on tile type preference
            const styleFile = this.options.useVectorTiles ? './cartography-vector.json' : './cartography.json';
            const response = await fetch(styleFile);
            
            if (!response.ok) {
                // Fallback to main style if vector style doesn't exist
                if (this.options.useVectorTiles) {
                    console.warn('Vector tile style not found, falling back to PMTiles style');
                    const fallbackResponse = await fetch('./cartography.json');
                    if (!fallbackResponse.ok) {
                        throw new Error(`Failed to load style: ${fallbackResponse.statusText}`);
                    }
                    const style = await fallbackResponse.json();
                    this.updatePMTilesUrls(style);
                    await this.addContourToStyle(style);
                    this.sortLayersByDrawOrder(style);
                    return style;
                }
                throw new Error(`Failed to load style: ${response.statusText}`);
            }
            
            const style = await response.json();
            
            // Update URLs based on tile type
            if (this.options.useVectorTiles) {
                this.updateVectorTileUrls(style);
            } else {
                this.updatePMTilesUrls(style);
            }
            
            // Add contour sources and layers to the style
            await this.addContourToStyle(style);
            
            // Sort layers according to draw order
            this.sortLayersByDrawOrder(style);
                        
            return style;
        } catch (error) {
            console.error('Error loading style:', error);
            // Fallback to a basic style if loading fails
            return this.getBasicStyle();
        }
    }

    /**
     * Update PMTiles URLs to be absolute paths for production deployment
     * Also adds error handling for GitHub Pages hosting issues
     */
    updatePMTilesUrls(style) {
        const isLocalhost = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1';
        const isGitHubPages = window.location.hostname.includes('github.io');
        
        // For GitHub Pages, we need to include the repo name in the path
        const repoName = isGitHubPages ? window.location.pathname.split('/')[1] : '';
        const basePath = isGitHubPages ? `/${repoName}` : '';
        
        // console.log('Updating PMTiles URLs:', { 
        //     isLocalhost, 
        //     isGitHubPages, 
        //     repoName, 
        //     basePath 
        // });
        
        for (const [sourceId, source] of Object.entries(style.sources)) {
            if (source.type === 'vector' && source.url && source.url.startsWith('pmtiles://tiles/')) {
                const tilePath = source.url.replace('pmtiles://tiles/', '');
                
                let newUrl;
                if (isLocalhost) {
                    // Localhost: use relative path
                    newUrl = `pmtiles://./tiles/${tilePath}`;
                } else {
                    // Production: use absolute path with proper base
                    newUrl = `pmtiles://${basePath}/tiles/${tilePath}`;
                }
                
                // console.log(`${sourceId}: ${source.url} → ${newUrl}`);
                source.url = newUrl;
                
                // // Add warning for GitHub Pages users
                // if (isGitHubPages) {
                //     console.warn(`PMTiles on GitHub Pages may have byte-serving issues. Consider using a CDN for ${sourceId}.`);
                // }
            }
        }
        
        // // If on GitHub Pages, add error handling for missing tiles
        // if (isGitHubPages) {
        //     this.addGitHubPagesWarning();
        // }
    }
    
    /**
     * Update vector tile URLs for traditional tile serving
     */
    updateVectorTileUrls(style) {
        const baseUrl = window.location.origin + window.location.pathname.replace(/\/[^\/]*$/, '');
        
        for (const [sourceId, source] of Object.entries(style.sources)) {
            if (source.type === 'vector' && source.tiles) {
                // Update relative URLs to absolute
                source.tiles = source.tiles.map(tileUrl => {
                    if (tileUrl.startsWith('./') || tileUrl.startsWith('/')) {
                        return `${baseUrl}${tileUrl.replace('./', '/')}`;
                    }
                    return tileUrl;
                });
            }
        }
    }
    
    // /**
    //  * Add warning about GitHub Pages limitations
    //  */
    // addGitHubPagesWarning() {
    //     console.warn('⚠️  PMTiles on GitHub Pages Notice:');
    //     console.warn('GitHub Pages may not properly support HTTP range requests required by PMTiles.');
    //     console.warn('For better performance, consider hosting tiles on:');
    //     console.warn('• Protomaps Cloud (free tier available)');
    //     console.warn('• Cloudflare R2 or AWS S3');
    //     console.warn('• Converting to traditional vector tile directories');
    //     console.warn('See DEPLOYMENT_OPTIONS.md for details.');
    // }
    
    /**
     * Show user-friendly error message for PMTiles issues
     */
    showPMTilesError() {
        const mapContainer = document.getElementById(this.containerId);
        const errorOverlay = document.createElement('div');
        errorOverlay.style.cssText = `
            position: absolute;
            top: 10px;
            left: 10px;
            right: 10px;
            background: rgba(231, 76, 60, 0.95);
            color: white;
            padding: 15px;
            border-radius: 5px;
            font-family: sans-serif;
            font-size: 14px;
            z-index: 10000;
            box-shadow: 0 2px 10px rgba(0,0,0,0.3);
        `;
        errorOverlay.innerHTML = `
            <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                <div>
                    <strong>⚠️ Tile Loading Issue</strong><br>
                    GitHub Pages doesn't fully support the byte-serving required by PMTiles.<br>
                    <small>Consider hosting tiles on a CDN for better reliability.</small>
                </div>
                <button onclick="this.parentElement.parentElement.remove()" style="background: none; border: none; color: white; font-size: 18px; cursor: pointer; padding: 0 5px;">×</button>
            </div>
        `;
        mapContainer.appendChild(errorOverlay);
        
        // Auto-remove after 10 seconds
        setTimeout(() => {
            if (errorOverlay.parentElement) {
                errorOverlay.remove();
            }
        }, 10000);
    }
    
    /**
     * Create the MapLibre map instance
     */
    createMap(style) {
        // Detect if user is on a mobile device
        const isMobile = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent) || 
                         ('ontouchstart' in window) || 
                         (navigator.maxTouchPoints > 0);
        
        // Configure interaction options for better mobile experience
        const interactionOptions = {
            // Only disable keyboard on mobile, let MapLibre handle desktop defaults
            ...(isMobile ? {
                keyboard: false, // Disable keyboard controls on mobile
                // Touch-specific optimizations
                touchZoomRotate: true,
                touchPitch: true,
                dragPan: {
                    deceleration: 2400,  // Faster deceleration for more responsive feel (default: 1400)
                },
                scrollZoom: {
                    around: 'center' // center point zoom
                },
                pitchWithRotate: false,  // Disable pitch on rotate for simpler interaction
                bearingSnap: 7          // Snap to cardinal directions more easily
            } : {
                // Desktop: Use MapLibre defaults by not overriding anything
                // This ensures the smoothest possible desktop experience
            })
        };

        this.map = new maplibregl.Map({
            container: this.containerId,
            style: style,
            // Use custom center if enabled, otherwise we'll set it after tiles load
            ...(this.options.useCustomCenter ? {
                center: this.options.center,
                zoom: this.options.zoom
            } : {
                bounds: this.options.bounds,
                fitBoundsOptions: {
                    padding: 20
                }
            }),
            minZoom: this.options.minZoom,
            maxZoom: this.options.maxZoom,
            
            // Clamp camera to bounds if enabled
            ...(this.options.clampToBounds ? {
                maxBounds: this.options.bounds
            } : {}),
            
            // Disable default attribution control so we can add custom one
            attributionControl: false,
            
            // Apply interaction optimizations
            ...interactionOptions,
            
            // Performance optimizations for mobile
            ...(isMobile ? {
                antialias: false, // performance
                failIfMajorPerformanceCaveat: false,
                preserveDrawingBuffer: false,
                fadeDuration: 50, 
                crossSourceCollisions: false,  // performance
                optimizeForTerrain: false, // Disable terrain optimization for better touch performance
                renderWorldCopies: false, // performance
                refreshExpiredTiles: false // performance
            } : {})
        });
        
        // Additional mobile optimizations after map creation
        if (isMobile) {
            // Add touch-specific event listeners for better responsiveness
            this.setupMobileTouchOptimizations();
        }
        
        this.map.showTileBoundaries = this.options.showTileBoundaries;
    }
    
    /**
     * Setup map event handlers
     */
    setupEventHandlers() {
        // Map load event
        this.map.on('load', () => {
            console.log('Map loaded successfully!');
            // console.log('Available sources:', Object.keys(this.map.getStyle().sources));
            
            // Check if layers exist and are visible
            const layers = this.map.getStyle().layers;
            console.log('All layers loaded:', layers.map(l => ({
                id: l.id,
                type: l.type,
                source: l.source,
                visibility: l.layout?.visibility || 'visible'
            })));
            
            // const contoursLayer = layers.find(layer => layer.id === 'contours');
            // const hillshadeLayer = layers.find(layer => layer.id === 'hills');
            
            // console.log('Contours layer found:', contoursLayer ? 'Yes' : 'No');
            // console.log('Hillshade layer found:', hillshadeLayer ? 'Yes' : 'No');
            
            // Check for PMTiles layers
            // const pmtilesLayers = layers.filter(layer => 
            //     layer.source && layer.source.includes('tiles') && 
            //     !layer.source.includes('contours') && 
            //     !layer.source.includes('dem')
            // );
            // console.log('PMTiles layers found:', pmtilesLayers.map(l => l.id));
            
            // debugging
            // this.printLayerOrder();
            
            
            // Auto-center to bounds if useCustomCenter is false
            if (!this.options.useCustomCenter) {
                this.map.fitBounds(this.options.bounds, { padding: 20 });
            }
            
            // Update camera bounds if clampToBounds is enabled
            if (this.options.clampToBounds) {
                console.log('Camera bounds set to:', this.options.bounds);
                this.map.setMaxBounds(this.options.bounds);
            }
            
            // Debug: Force layer visibility after load
            // setTimeout(() => {
            //     const layers = ['land-use', 'land', 'water-polygons', 'water-lines', 'roads-solid', 'buildings'];
            //     layers.forEach(layerId => {
            //         if (this.map.getLayer(layerId)) {
            //             this.map.setLayoutProperty(layerId, 'visibility', 'visible');
            //             console.log(`Forced ${layerId} to visible`);
            //         } else {
            //             console.warn(`Layer ${layerId} not found`);
            //         }
            //     });
            // }, 1000);
        });
        
        // Add PMTiles-specific error handling
        this.map.on('error', (e) => {
            if (e.error && e.error.message) {
            const errorMsg = e.error.message;
            const sourceId = e.sourceId || 'unknown source';
            const tileUrl = e.tile || 'unknown tile';
            
            if (errorMsg.includes('content-length') || errorMsg.includes('Byte Serving')) {
                console.error(`PMTiles byte-serving error detected for source: ${sourceId}, tile: ${tileUrl}`);
                console.error('Error message:', errorMsg);
                console.error('This is likely due to hosting limitations. See DEPLOYMENT_OPTIONS.md for solutions.');
                
                // Show user-friendly message
                this.showPMTilesError();
            } else {
                console.error(`Map error detected for source: ${sourceId}, tile: ${tileUrl}`);
                console.error('Error message:', errorMsg);
            }
            }
        });
        
        // Source loading feedback
        // this.map.on('sourcedataloading', (e) => {
        //     if (e.sourceId && e.sourceId.includes('tiles')) {
        //         console.log(`Loading ${e.sourceId}...`);
        //     }
        // });
        
        this.map.on('sourcedata', (e) => {
            if (e.sourceId && e.isSourceLoaded && e.sourceId.includes('tiles')) {
                // console.log(`✓ ${e.sourceId} loaded successfully`);
                
                // Debug: Check if source has data
                // const source = this.map.getSource(e.sourceId);
                // if (source) {
                //     console.log(`Source ${e.sourceId} details:`, {
                //         type: source.type,
                //         url: source._options?.url,
                //         loaded: e.isSourceLoaded
                //     });
                // }
                
                // Debug: Query features from this source after it loads
                // setTimeout(() => {
                //     try {
                //         const layersFromThisSource = this.map.getStyle().layers
                //             .filter(layer => layer.source === e.sourceId)
                //             .map(layer => layer.id);
                        
                //         // Log current map state
                //         console.log(`Current map center: [${this.map.getCenter().lng.toFixed(4)}, ${this.map.getCenter().lat.toFixed(4)}]`);
                //         console.log(`Current map zoom: ${this.map.getZoom().toFixed(2)}`);
                //         console.log(`Current map bounds:`, this.map.getBounds());
                        
                //         if (layersFromThisSource.length > 0) {
                //             const features = this.map.queryRenderedFeatures({
                //                 layers: layersFromThisSource
                //             });
                //             console.log(`Features visible from ${e.sourceId}:`, features.length);
                //             if (features.length > 0) {
                //                 console.log(`Sample feature from ${e.sourceId}:`, features[0]);
                //             } else {
                //                 console.warn(`No features visible from ${e.sourceId} - this might indicate an issue`);
                                
                //                 // Additional debugging: Check source-layer configuration
                //                 const sourceLayerInfo = layersFromThisSource.map(layerId => {
                //                     const layer = this.map.getLayer(layerId);
                //                     return {
                //                         layerId: layerId,
                //                         sourceLayer: layer['source-layer'],
                //                         type: layer.type,
                //                         visibility: layer.layout?.visibility || 'visible'
                //                     };
                //                 });
                //                 console.log(`Source layer config for ${e.sourceId}:`, sourceLayerInfo);
                                
                //                 // Try querying without specifying layers to see if any features exist
                //                 const allVisibleFeatures = this.map.queryRenderedFeatures();
                //                 const featuresFromThisSource = allVisibleFeatures.filter(f => f.source === e.sourceId);
                //                 console.log(`Total features from ${e.sourceId} (any layer):`, featuresFromThisSource.length);
                                
                //                 if (featuresFromThisSource.length > 0) {
                //                     console.log(`Sample feature (any layer) from ${e.sourceId}:`, featuresFromThisSource[0]);
                //                     console.log(`Source layer name found: "${featuresFromThisSource[0].sourceLayer}"`);
                //                 } else {
                //                     // The issue might be that we're not looking at the right geographic area
                //                     // Let's check if the PMTiles data covers the current view
                //                     console.log(`PMTiles file bounds from tippecanoe-decode suggest data around: 14.5-16.5°E, -5.0 to -3.0°N (DRC)`);
                //                     console.log(`Current view is centered at: [${this.map.getCenter().lng.toFixed(4)}, ${this.map.getCenter().lat.toFixed(4)}]`);
                //                     console.log(`Consider updating the map center to match the data bounds or regenerating tiles for your area of interest.`);
                //                 }
                //             }
                //         }
                //     } catch (error) {
                //         console.error(`Error querying features from ${e.sourceId}:`, error);
                //     }
                // }, 500);
            }
        });
        
        // Source data events
        // this.map.on('sourcedata', (e) => {
        //     if (e.sourceId === 'roads-tiles' && e.isSourceLoaded) {
        //         console.log('Roads tiles loaded successfully!');
        //         // Check if roads are visible at current zoom/extent
        //         setTimeout(() => {
        //             const features = this.map.queryRenderedFeatures({layers: ['roads']});
        //             console.log('Roads features visible:', features.length);
        //             if (features.length > 0) {
        //                 console.log('Sample road feature:', features[0]);
        //             }
        //         }, 1000);
        //     }
        // });
        
        // Click event for feature inspection
        // this.map.on('click', (e) => {
        //     const features = this.map.queryRenderedFeatures(e.point);
        //     if (features.length > 0) {
        //         const feature = features[0];
        //         console.log('Clicked feature:', feature);
                
        //         // Create popup with feature info
        //         new maplibregl.Popup()
        //             .setLngLat(e.lngLat)
        //             .setHTML(this.formatFeaturePopup(feature))
        //             .addTo(this.map);
        //     }
        // });
    }
    
    // controls
    addControls() {
        // Detect if user is on a mobile device (same logic as in createMap)
        const isMobile = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent) || 
                         ('ontouchstart' in window) || 
                         (navigator.maxTouchPoints > 0);
        
        // Add navigation controls
        this.map.addControl(new maplibregl.NavigationControl(), 'top-right');
        
        // Add scale control
        this.map.addControl(new maplibregl.ScaleControl(), 'bottom-left');
        
        // Add custom attribution control with responsive compact setting
        this.map.addControl(new maplibregl.AttributionControl({
            customAttribution: [
                '© <a href="https://overturemaps.org/" target="_blank">Overture Maps Foundation</a>',
                '© <a href="https://www.openstreetmap.org/copyright" target="_blank">OpenStreetMap contributors</a>',
                'Contours: <a href="https://www.usgs.gov/" target="_blank">USGS</a>'
            ],
            compact: isMobile // Compact on mobile, expanded on desktop
        }), 'bottom-right');
        
        // Add layer legend control
        this.addLayerLegend();
    }
    
    
    // popups, later
    formatFeaturePopup(feature) {
        let content = `
            <div style="max-width: 200px;">
                <strong>Layer:</strong> ${feature.sourceLayer || feature.source}<br>
        `;
        
        // Special handling for contour features
        if (feature.sourceLayer === 'contours') {
            const elevation = feature.properties.ele;
            const level = feature.properties.level;
            content += `
                <strong>Elevation:</strong> ${elevation}' (${Math.round(elevation * 0.3048)}m)<br>
                <strong>Contour Type:</strong> ${level > 0 ? 'Major' : 'Minor'}<br>
            `;
        }
        
        content += `
                <strong>Properties:</strong><br>
                <pre style="font-size: 10px; white-space: pre-wrap;">${JSON.stringify(feature.properties, null, 2)}</pre>
            </div>
        `;
        
        return content;
    }
    
    // fallback
    getBasicStyle() {
        return {
            version: 8,
            glyphs: "https://fonts.openmaptiles.org/{fontstack}/{range}.pbf",
            sources: {},
            layers: [
                {
                    id: 'background',
                    type: 'background',
                    paint: {
                        'background-color': '#f0f0f0'
                    }
                }
            ]
        };
    }
    
    // append third party contours to cartographic style
    async addContourToStyle(style) {
        // Lazy load contour functionality
        const { demSource } = await initContours();
        
        // Add DEM source for hillshade
        style.sources.dem = {
            type: "raster-dem",
            encoding: "terrarium",
            tiles: [demSource.sharedDemProtocolUrl], // share cached DEM tiles with contour layer
            maxzoom: 13,
            tileSize: 256
        };
        
        // Add contour source
        style.sources.contours = {
            type: "vector",
            tiles: [
                demSource.contourProtocolUrl({
                    // meters to feet conversion for US data
                    multiplier: 3.28084,
                    thresholds: {
                        // zoom: [minor, major] contour intervals in feet
                        11: [100, 400],
                        12: [50, 200],
                        13: [50, 200],
                        14: [25, 100],
                        15: [12.5, 100],
                        16: [5, 100]
                    },
                    elevationKey: "ele",
                    levelKey: "level",
                    contourLayer: "contours"
                })
            ],
            maxzoom: 16
        };
        
        // Insert hillshade layer after background but before other layers
        const hillshadeLayer = {
            id: "hills",
            type: "hillshade",
            source: "dem",
            paint: {
            "hillshade-exaggeration": [
                "interpolate",
                ["linear"],
                ["zoom"],
                0, 1,
                9, 0.7,
                11, 0.4,
                16, 0.15
            ],
            "hillshade-shadow-color": [
                "interpolate",
                ["linear"],
                ["zoom"],
                0, "rgba(0,0,0,0.35)",
                11, "rgba(0,0,0,0.15)",
                16, "rgba(123, 123, 123, 0.1)"
            ],
            "hillshade-highlight-color": [
                "interpolate",
                ["linear"],
                ["zoom"],
                11, "rgba(255,255,255,0.15)",
                13, "rgba(255,255,255,.5)",
                16, "rgba(239, 239, 239, 0.2)"
            ]
            }
        };
        
        // mix-blend-mode approximation
        // these opacity settings are the ones applied
        const contourLinesLayer = {
            id: "contours",
            type: "line",
            source: "contours",
            "source-layer": "contours",
            paint: {
                "line-color": "rgba(139, 69, 19, 0.4)",  // Neutral brown
                "line-width": [
                    "interpolate",
                    ["linear"],
                    ["zoom"],
                    8.5, [
                        "case",
                        ["==", ["get", "level"], 1], 0.15,  // Major contours
                        0.07                                 // Minor contours
                    ],
                    10.5, [
                        "case", 
                        ["==", ["get", "level"], 1], 0.35,  // Major contours
                        0.2                               // Minor contours
                    ],
                    13.5, [
                        "case",
                        ["==", ["get", "level"], 1], 0.6,  // Major contours
                        0.3                              // Minor contours
                    ]
                ],
                "line-opacity": 1  // Simple neutral opacity - will be overridden
            },
            layout: {
                "line-join": "round",
                "line-cap": "round"
            }
        };
        
        // contour labels, hidden rn
        const contourLabelsLayer = {
            id: "contour-text",
            type: "symbol",
            source: "contours",
            "source-layer": "contours",
            filter: [">", ["get", "level"], 0],
            paint: {
                "text-halo-color": "white",
                "text-halo-width": 2,
                "text-color": "rgba(139, 69, 19, 0.8)"
            },
            layout: {
                "visibility": "none",
                "symbol-placement": "line",
                "text-anchor": "center",
                "text-size": 10,
                "text-field": [
                    "concat",
                    ["number-format", ["get", "ele"], {}],
                    "'"
                ],
                "text-font": ["Noto Sans Bold"],
                "text-rotation-alignment": "map"
            }
        };
        
        // Add the contour and hillshade layers to the style
        // The sorting will be handled by sortLayersByDrawOrder() method
        style.layers.push(hillshadeLayer);
        style.layers.push(contourLinesLayer);
        style.layers.push(contourLabelsLayer);
    }

    /**
     * Sort layers according to the draw order index
     * @param {Object} style - The MapLibre style object
     * @returns {Object} - Style with sorted layers
     */
    sortLayersByDrawOrder(style) {
        if (!style.layers) return style;
        
        // Sort layers based on draw order index
        style.layers.sort((a, b) => {
            const orderA = this.layerDrawOrder[a.id] !== undefined ? this.layerDrawOrder[a.id] : 999;
            const orderB = this.layerDrawOrder[b.id] !== undefined ? this.layerDrawOrder[b.id] : 999;
            return orderA - orderB;
        });
        
        // console.log('Layer draw order applied:', style.layers.map(layer => ({
        //     id: layer.id,
        //     order: this.layerDrawOrder[layer.id] || 'unspecified'
        // })));
        
        return style;
    }
    
    /**
     * Add a new layer with specified draw order
     * @param {string} layerId - The layer ID
     * @param {number} drawOrder - The draw order index (0 = bottom, higher = top)
     * @param {Object} layerDefinition - The layer definition object
     */
    addLayerWithOrder(layerId, drawOrder, layerDefinition) {
        if (!this.map) return;
        
        // Update the draw order index
        this.layerDrawOrder[layerId] = drawOrder;
        
        // Find the correct position to insert the layer
        const sortedLayers = Object.entries(this.layerDrawOrder)
            .filter(([id, order]) => this.map.getLayer(id) && order <= drawOrder)
            .sort((a, b) => b[1] - a[1]); // Sort descending to find the layer just below
        
        const beforeLayerId = sortedLayers.length > 0 ? sortedLayers[0][0] : undefined;
        
        // Add the layer
        this.map.addLayer(layerDefinition, beforeLayerId);
        
        console.log(`Added layer '${layerId}' with draw order ${drawOrder}`);
    }
    
    /**
     * Update layer draw order
     * @param {string} layerId - The layer ID
     * @param {number} newDrawOrder - The new draw order index
     */
    updateLayerOrder(layerId, newDrawOrder) {
        if (!this.map || !this.map.getLayer(layerId)) return;
        
        // Update the draw order index
        this.layerDrawOrder[layerId] = newDrawOrder;
        
        // Remove and re-add the layer to change its position
        const layerDefinition = this.map.getLayer(layerId);
        this.map.removeLayer(layerId);
        this.addLayerWithOrder(layerId, newDrawOrder, layerDefinition);
    }
    
    /**
     * Get the map instance
     */
    getMap() {
        return this.map;
    }
    
    /**
     * Toggle layer visibility
     */
    toggleLayer(layerId, visible = null) {
        if (!this.map) return;
        
        const visibility = visible !== null ? 
            (visible ? 'visible' : 'none') : 
            (this.map.getLayoutProperty(layerId, 'visibility') === 'none' ? 'visible' : 'none');
        
        this.map.setLayoutProperty(layerId, 'visibility', visibility);
    }
    
    /**
     * Toggle contour layers visibility
     */
    toggleContours(visible = null) {
        this.toggleLayer('contours', visible);
        this.toggleLayer('contour-text', visible);
    }
    
    /**
     * Toggle settlement extents layers visibility
     */
    toggleSettlementExtents(visible = null) {
        this.toggleLayer('settlement-extents-fill', visible);
        this.toggleLayer('settlement-extents-outlines', visible);
    }
    
    /**
     * Toggle hillshade visibility
     */
    toggleHillshade(visible = null) {
        this.toggleLayer('hills', visible);
    }
    
    /**
     * Set contour interval based on zoom level
     */
    setContourInterval(minorInterval, majorInterval) {
        if (!this.map) return;
        
        // Update the contour source with new thresholds
        const currentZoom = Math.floor(this.map.getZoom());
        const newThresholds = {};
        newThresholds[currentZoom] = [minorInterval, majorInterval];
        
        // Note: Changing contour intervals requires reloading the source
        console.log(`Contour intervals set to: minor=${minorInterval}ft, major=${majorInterval}ft`);
    }

    // /**
    //  * Set contour blend mode (simulated through color adjustments)
    //  * @param {string} mode - 'darken', 'multiply', 'overlay', 'normal'
    //  */
    // setContourBlendMode(mode = 'darken') {
    //     // Enhanced error checking and debugging
    //     if (!this.map) {
    //         console.warn('Map not initialized for contour blend mode');
    //         return;
    //     }
        
    //     if (!this.map.getLayer('contours')) {
    //         console.warn('Contours layer not found. Available layers:', 
    //             this.map.getStyle().layers.map(l => l.id));
    //         return;
    //     }
        
    //     // Check if layer is loaded
    //     if (!this.map.isSourceLoaded('contours')) {
    //         console.warn('Contours source not yet loaded, retrying in 500ms...');
    //         setTimeout(() => this.setContourBlendMode(mode), 500);
    //         return;
    //     }
        
    //     let colorExpression, opacityValue;
        
    //     switch (mode) {
    //         case 'darken':
    //             colorExpression = [
    //                 "interpolate",
    //                 ["linear"],
    //                 ["zoom"],
    //                 11, "rgba(0, 0, 0, 0.2)",      // Increased alpha for more visibility
    //                 13, "rgba(50, 25, 0, 0.4)",    // Increased alpha
    //                 15, "rgba(93, 55, 79, 0.6)"    // Increased alpha
    //             ];
    //             opacityValue = [
    //                 "interpolate",
    //                 ["linear"],
    //                 ["zoom"],
    //                 11, 0.8,   // Increased opacity
    //                 13, 0.9,   // Increased opacity
    //                 15, 1.0    // Full opacity at high zoom
    //             ];
    //             break;
                
    //         case 'multiply':
    //             colorExpression = [
    //                 "interpolate",
    //                 ["linear"],
    //                 ["zoom"],
    //                 11, "rgba(40, 20, 10, 0.4)",   // Increased alpha
    //                 13, "rgba(60, 30, 15, 0.6)",   // Increased alpha
    //                 15, "rgba(80, 40, 20, 0.8)"    // Increased alpha
    //             ];
    //             opacityValue = 1.0;  // Full opacity for multiply effect
    //             break;
                
    //         case 'overlay':
    //             colorExpression = [
    //                 "interpolate",
    //                 ["linear"],
    //                 ["zoom"],
    //                 11, "rgba(139, 69, 19, 0.3)",  // Increased alpha
    //                 13, "rgba(160, 80, 40, 0.5)",  // Increased alpha
    //                 15, "rgba(180, 90, 45, 0.7)"   // Increased alpha
    //             ];
    //             opacityValue = 0.8;  // Increased base opacity
    //             break;
                
    //         case 'normal':
    //         default:
    //             colorExpression = "rgba(139, 69, 19, 0.6)";  // Increased alpha
    //             opacityValue = 1.0;  // Full opacity
    //             break;
    //     }
        
    //     try {
    //         // Get current properties for comparison
    //         const currentColor = this.map.getPaintProperty('contours', 'line-color');
    //         const currentOpacity = this.map.getPaintProperty('contours', 'line-opacity');
            
    //         console.log('Current contour properties:', {
    //             color: currentColor,
    //             opacity: currentOpacity
    //         });
            
    //         // Apply new properties
    //         this.map.setPaintProperty('contours', 'line-color', colorExpression);
    //         this.map.setPaintProperty('contours', 'line-opacity', opacityValue);
            
    //         // Verify the change was applied
    //         setTimeout(() => {
    //             const newColor = this.map.getPaintProperty('contours', 'line-color');
    //             const newOpacity = this.map.getPaintProperty('contours', 'line-opacity');
                
    //             console.log(`Contour blend mode set to: ${mode}`, {
    //                 newColor,
    //                 newOpacity,
    //                 currentZoom: this.map.getZoom().toFixed(2)
    //             });
    //         }, 100);
            
    //     } catch (error) {
    //         console.error('Error setting contour blend mode:', error);
    //     }
    // }

    /**
     * Get current contour paint properties (for debugging)
     */
    getContourProperties() {
        if (!this.map || !this.map.getLayer('contours')) {
            return 'Contours layer not available';
        }
        
        return {
            color: this.map.getPaintProperty('contours', 'line-color'),
            opacity: this.map.getPaintProperty('contours', 'line-opacity'),
            width: this.map.getPaintProperty('contours', 'line-width'),
            zoom: this.map.getZoom(),
            sourceLoaded: this.map.isSourceLoaded('contours'),
            layerVisible: this.map.getLayoutProperty('contours', 'visibility') !== 'none'
        };
    }

    /**
     * Cleanup resources
     */
    destroy() {
        if (!this.map) {
            this.map.remove();
        }
        if (this.protocol) {
            maplibregl.removeProtocol("pmtiles");
        }
    }
    
    /**
     * Get the current layer draw order configuration
     * @returns {Object} - The layer draw order index
     */
    getLayerDrawOrder() {
        return { ...this.layerDrawOrder };
    }
    
    /**
     * Get layers sorted by draw order
     * @returns {Array} - Array of layer IDs in draw order
     */
    getLayersByDrawOrder() {
        if (!this.map) return [];
        
        const layers = this.map.getStyle().layers;
        return layers
            .map(layer => ({
                id: layer.id,
                order: this.layerDrawOrder[layer.id] || 999
            }))
            .sort((a, b) => a.order - b.order)
            .map(item => item.id);
    }
    
    /**
     * Print current layer order to console (for debugging)
     */
    printLayerOrder() {
        if (!this.map) {
            console.log('Map not initialized');
            return;
        }
        
        const layers = this.map.getStyle().layers;
        console.log('Current layer stack (bottom to top):');
        console.table(layers.map((layer, index) => ({
            Position: index,
            'Layer ID': layer.id,
            'Draw Order': this.layerDrawOrder[layer.id] || 'unspecified',
            Type: layer.type
        })));
    }
    
    /**
     * Setup mobile-specific touch optimizations
     */
    setupMobileTouchOptimizations() {
        if (!this.map) return;
        
        // Optimize canvas for touch interactions
        const canvas = this.map.getCanvas();
        canvas.style.touchAction = 'pan-x pan-y';
        
        // Improve rendering performance during touch interactions
        this.map.on('touchstart', () => {
            // Enable hardware acceleration during touch
            canvas.style.willChange = 'transform';
        });
        
        this.map.on('touchend', () => {
            // Restore normal rendering after touch interaction
            setTimeout(() => {
                canvas.style.willChange = 'auto';
            }, 200);
        });
        
        // Optimize rendering during zoom for better performance
        let isZooming = false;
        this.map.on('zoomstart', () => {
            isZooming = true;
            // Temporarily reduce rendering quality during zoom
            canvas.style.imageRendering = 'pixelated';
        });
        
        this.map.on('zoomend', () => {
            if (isZooming) {
                isZooming = false;
                // Restore high-quality rendering after zoom
                canvas.style.imageRendering = 'auto';
                // Force a repaint to ensure crisp rendering
                setTimeout(() => {
                    this.map.triggerRepaint();
                }, 50);
            }
        });
        
        // Optimize move events for better performance
        let isDragging = false;
        this.map.on('movestart', (e) => {
            if (e.originalEvent && e.originalEvent.type === 'touchstart') {
                isDragging = true;
                // Reduce quality during drag for better frame rate
                canvas.style.imageRendering = 'optimizeSpeed';
            }
        });
        
        this.map.on('moveend', () => {
            if (isDragging) {
                isDragging = false;
                // Restore quality after drag
                canvas.style.imageRendering = 'auto';
                setTimeout(() => {
                    this.map.triggerRepaint();
                }, 50);
            }
        });
    }
    
    /**
     * Toggle layer visibility
     */
    toggleLayerVisibility(layerId, visibility) {
        if (!this.map.getLayer(layerId)) {
            console.warn(`Layer with ID '${layerId}' does not exist.`);
            return;
        }
        this.map.setLayoutProperty(layerId, 'visibility', visibility);
    }

    /**
     * Mute a layer (hide it)
     */
    muteLayer(layerId) {
        this.toggleLayerVisibility(layerId, 'none');
    }

    /**
     * Solo a layer (show only this layer)
     */
    soloLayer(layerId) {
        const layers = this.map.getStyle().layers;
        layers.forEach(layer => {
            const currentVisibility = this.map.getLayoutProperty(layer.id, 'visibility');
            if (currentVisibility !== 'none') {
                this.toggleLayerVisibility(layer.id, layer.id === layerId ? 'visible' : 'none');
            }
        });
    }

    /**
     * Add layer legend to the map
     */
    addLayerLegend() {
        document.addEventListener('DOMContentLoaded', () => {
            const legendContainer = document.createElement('div');
            legendContainer.id = 'map-legend';
            legendContainer.style.position = 'absolute';
            legendContainer.style.top = '10px';
            legendContainer.style.right = '10px';
            legendContainer.style.backgroundColor = 'rgba(255, 255, 255, 0.8)';
            legendContainer.style.padding = '10px';
            legendContainer.style.borderRadius = '5px';
            legendContainer.style.boxShadow = '0 2px 4px rgba(0, 0, 0, 0.2)';

            const layers = [
                'background',
                'land-use',
                'land',
                'land-cover',
                'settlement-extents-fill',
                'settlement-extents-outlines',
                'water-polygons',
                'roads-solid',
                'buildings-low-lod',
                'buildings-medium-lod',
                'buildings-high-lod',
                'places'
            ];

            layers.forEach(layerId => {
                const layerItem = document.createElement('div');
                layerItem.style.display = 'flex';
                layerItem.style.alignItems = 'center';
                layerItem.style.marginBottom = '5px';

                const soloButton = document.createElement('div');
                soloButton.style.width = '10px';
                soloButton.style.height = '10px';
                soloButton.style.borderRadius = '50%';
                soloButton.style.backgroundColor = '#333';
                soloButton.style.marginRight = '5px';
                soloButton.style.cursor = 'pointer';
                soloButton.title = `Solo ${layerId}`;
                soloButton.addEventListener('click', () => {
                    this.soloLayer(layerId);
                });

                const layerName = document.createElement('span');
                layerName.textContent = layerId;
                layerName.style.cursor = 'pointer';
                layerName.title = `Toggle ${layerId}`;
                layerName.addEventListener('click', () => {
                    const currentVisibility = this.getMap().getLayoutProperty(layerId, 'visibility');
                    this.toggleLayer(layerId, currentVisibility === 'none');
                });

                layerItem.appendChild(soloButton);
                layerItem.appendChild(layerName);
                legendContainer.appendChild(layerItem);
            });

            document.body.appendChild(legendContainer);
        });
    }
}

/**
 * Auto-cleanup on page unload
 */
window.addEventListener('beforeunload', () => {
    maplibregl.removeProtocol("pmtiles");
});

// Export the class as default
export default OvertureMap;
