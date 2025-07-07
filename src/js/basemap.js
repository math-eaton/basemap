import maplibregl from 'maplibre-gl';
import { Protocol } from 'pmtiles';

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
            // Default bounds for St. Lawrence County
            bounds: [
                [-75.5, 44.0], // Southwest coordinates [lng, lat]
                [-74.5, 45.0]  // Northeast coordinates [lng, lat]
            ],
            center: [-74.986763650502, 44.66997929549087],
            zoom: 13,
            minZoom: 11,
            maxZoom: 16,
            showTileBoundaries: false,
            clampToBounds: true, // limit view to bounds
            ...options
        };
        
        this.map = null;
        this.protocol = null;
        
        // Layer draw order index - lower numbers draw first (bottom), higher numbers draw on top
        this.layerDrawOrder = {
            // Base layers (0-9)
            'background': 0,
            
            // Land use and land cover (20-39)
            'land-use': 20,       // Land use polygons (residential, commercial, etc.)
            'land': 25,           // Natural land features (forest, grass, etc.)
            
            // Water features (40-49)
            'water-polygons': 40,        // Water body fills
            'water-polygon-outlines': 41, // Water body outlines
            'water-lines': 42,           // Rivers, streams, canals
                        
            // Terrain and elevation 
            'hills': 47,

            // Contour lines (50-59)
            'contours': 50,       // Contour lines
            'contour-text': 51,   // Contour elevation labels
            
            // Transportation (60-79)
            'roads-solid': 60,    // Major road lines (solid)
            'roads-dashed': 61,    // Minor road lines (dashed)
            
            // Buildings and structures (80-89)
            'buildings': 80,           // Building fills
            'building-outlines': 81,   // Building outlines
            
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
            this.setupContourControls();
        }).catch(error => {
            console.error('Failed to load map style:', error);
        });
    }
    
    /**
     * Load the MapLibre style from JSON file
     */
    async loadStyle() {
        try {
            const response = await fetch('./cartography.json');
            if (!response.ok) {
                throw new Error(`Failed to load style: ${response.statusText}`);
            }
            const style = await response.json();
            
            // Update PMTiles URLs to be absolute for production
            this.updatePMTilesUrls(style);
            
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
     */
    updatePMTilesUrls(style) {
        const baseUrl = window.location.origin + window.location.pathname.replace(/\/[^\/]*$/, '');
        
        for (const [sourceId, source] of Object.entries(style.sources)) {
            if (source.type === 'vector' && source.url && source.url.startsWith('pmtiles://tiles/')) {
                const tilePath = source.url.replace('pmtiles://tiles/', '');
                source.url = `pmtiles://${baseUrl}/tiles/${tilePath}`;
            }
        }
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
            // bounds: this.options.bounds,
            center: this.options.center,
            zoom: this.options.zoom,
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
            // console.log('Map loaded successfully!');
            // console.log('Available sources:', this.map.getStyle().sources);
            
            // Check if layers exist
            const layers = this.map.getStyle().layers;
            const contoursLayer = layers.find(layer => layer.id === 'contours');
            const hillshadeLayer = layers.find(layer => layer.id === 'hills');
            
            // console.log('Contours layer found:', contoursLayer ? 'Yes' : 'No');
            // console.log('Hillshade layer found:', hillshadeLayer ? 'Yes' : 'No');
            
            // debugging
            this.printLayerOrder();
            
            // contour controls now that map is loaded
            this.setupContourControls();
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
    }
    
    // contour controls (gui, later)
    setupContourControls() {
        if (!this.map) {
            console.warn('Map not yet initialized for contour controls');
            return;
        }
        
        // Contour toggle
        const contoursToggle = document.getElementById('contoursToggle');
        if (contoursToggle) {
            contoursToggle.addEventListener('change', (e) => {
                this.toggleContours(e.target.checked);
            });
        }
        
        // Hillshade toggle
        const hillshadeToggle = document.getElementById('hillshadeToggle');
        if (hillshadeToggle) {
            hillshadeToggle.addEventListener('change', (e) => {
                this.toggleHillshade(e.target.checked);
            });
        }
        
        // Blend mode selector
        const blendModeSelect = document.getElementById('blendModeSelect');
        if (blendModeSelect) {
            blendModeSelect.addEventListener('change', (e) => {
                this.setContourBlendMode(e.target.value);
            });
            
            // Set initial blend mode
            this.setContourBlendMode('darken');
        }
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
                "hillshade-exaggeration": 0.2,
                "hillshade-shadow-color": "rgba(0,0,0,0.2)",
                "hillshade-highlight-color": "rgba(255,255,255,0.2)"
            }
        };
        
        // mix-blend-mode approximation
        const contourLinesLayer = {
            id: "contours",
            type: "line",
            source: "contours",
            "source-layer": "contours",
            paint: {
                // Use dark colors with transparency to create darkening effect
                "line-color": [
                    "interpolate",
                    ["linear"],
                    ["zoom"],
                    11, "rgba(0, 0, 0, 0.4)",      // Very dark at low zoom
                    13, "rgba(50, 25, 0, 0.5)",    // Dark brown at medium zoom
                    15, "rgba(80, 40, 20, 0.6)"    // Medium brown at high zoom
                ],
                "line-width": [
                    "interpolate",
                    ["linear"],
                    ["zoom"],
                    11, [
                        "case",
                        ["==", ["get", "level"], 1], 0.7,  // Major contours
                        0.35                                 // Minor contours
                    ],
                    13, [
                        "case", 
                        ["==", ["get", "level"], 1], 0.8,  // Major contours
                        0.4                                // Minor contours
                    ],
                    15, [
                        "case",
                        ["==", ["get", "level"], 1], 1,  // Major contours
                        0.5                                // Minor contours
                    ]
                ],
                "line-opacity": [
                    "interpolate",
                    ["linear"],
                    ["zoom"],
                    11, 0.6,
                    13, 0.7,
                    15, 0.8
                ]
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
        
        console.log('Layer draw order applied:', style.layers.map(layer => ({
            id: layer.id,
            order: this.layerDrawOrder[layer.id] || 'unspecified'
        })));
        
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

    /**
     * Set contour blend mode (simulated through color adjustments)
     * @param {string} mode - 'darken', 'multiply', 'overlay', 'normal'
     */
    setContourBlendMode(mode = 'overlay') {
        if (!this.map || !this.map.getLayer('contours')) return;
        
        let colorExpression, opacityValue;
        
        switch (mode) {
            case 'darken':
                colorExpression = [
                    "interpolate",
                    ["linear"],
                    ["zoom"],
                    11, "rgba(0, 0, 0, 0.4)",
                    13, "rgba(50, 25, 0, 0.5)",
                    15, "rgba(80, 40, 20, 0.6)"
                ];
                opacityValue = [
                    "interpolate",
                    ["linear"],
                    ["zoom"],
                    11, 0.6,
                    13, 0.7,
                    15, 0.8
                ];
                break;
                
            case 'multiply':
                colorExpression = [
                    "interpolate",
                    ["linear"],
                    ["zoom"],
                    11, "rgba(40, 20, 10, 0.3)",
                    13, "rgba(60, 30, 15, 0.4)",
                    15, "rgba(80, 40, 20, 0.5)"
                ];
                opacityValue = 0.9;
                break;
                
            case 'overlay':
                colorExpression = [
                    "interpolate",
                    ["linear"],
                    ["zoom"],
                    11, "rgba(139, 69, 19, 0.2)",
                    13, "rgba(160, 80, 40, 0.4)",
                    15, "rgba(180, 90, 45, 0.5)"
                ];
                opacityValue = 0.5;
                break;
                
            case 'normal':
            default:
                colorExpression = "rgba(139, 69, 19, 0.6)";
                opacityValue = 1;
                break;
        }
        
        this.map.setPaintProperty('contours', 'line-color', colorExpression);
        this.map.setPaintProperty('contours', 'line-opacity', opacityValue);
        
        console.log(`Contour blend mode set to: ${mode}`);
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
}

/**
 * Auto-cleanup on page unload
 */
window.addEventListener('beforeunload', () => {
    maplibregl.removeProtocol("pmtiles");
});

// Export the class as default
export default OvertureMap;
