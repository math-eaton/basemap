/**
 * Main application entry point
 * Initializes the Overture map and sets up controls
 */

import '../styles/style.css';

// Wait for DOM and libraries to be loaded
document.addEventListener('DOMContentLoaded', async () => {
    // Show loading indicator
    const mapContainer = document.getElementById('map');
    mapContainer.innerHTML = '<div style="display: flex; align-items: center; justify-content: center; height: 100%; font-family: sans-serif; color: #666;">Loading map...</div>';
    
    try {
        // Dynamically import map dependencies
        const [
            { default: OvertureMap },
            maplibreStyles
        ] = await Promise.all([
            import('./basemap.js'),
            import('maplibre-gl/dist/maplibre-gl.css')
        ]);
        
        // Clear loading message
        mapContainer.innerHTML = '';
        
        // Detect if we're on GitHub Pages and might need to use vector tiles
        const isGitHubPages = window.location.hostname.includes('github.io');
        
        // Initialize the Overture map
        const overtureMap = new OvertureMap('map', {
            // Override default options here
            // bounds: [[-75.5, 44.0], [-74.5, 45.0]],
            // zoom: 14,
            // minZoom: 11,
            // maxZoom: 16,
            // clampToBounds: true // Uncomment to restrict camera movement to the defined bounds
            
            // Automatically try vector tiles on GitHub Pages as fallback
            // Set useVectorTiles: true if you have converted tiles available
            useVectorTiles: false, // Change to true if you have vector tile directories
        });
        
        // Make overtureMap available globally for debugging
        window.overtureMap = overtureMap;
        
        // Add convenience methods to global scope for testing
        window.toggleSettlementExtents = (visible) => overtureMap.toggleSettlementExtents(visible);
        window.toggleContours = (visible) => overtureMap.toggleContours(visible);
        window.toggleHillshade = (visible) => overtureMap.toggleHillshade(visible);
        
        console.log('Overture map initialization complete');
        
        // Ensure the legend is populated after the map is initialized
        const legendContainer = document.getElementById('map-legend');
        if (legendContainer) {
            const layers = [
                'background',
                'land-use',
                'land',
                'land-cover',
                'esri-admin0-forest',
                'esri-admin1-forest', 
                'esri-openspace-forest',
                'water-polygons',
                'esri-marsh-base',
                'esri-marsh-grass-1',
                'esri-marsh-grass-2',
                'esri-marsh-water-channels',
                'esri-marsh-outline',
                'roads-solid',
                'buildings-low-lod',
                'buildings-medium-lod',
                'buildings-high-lod',
                'settlement-extents-fill',
                'settlement-extents-outlines',
                'places',
                'health-facilities',
                'health-facilities-labels'
            ];

            layers.forEach(layerId => {
                const layerItem = document.createElement('div');
                layerItem.style.display = 'flex';
                layerItem.style.alignItems = 'center';
                layerItem.style.marginBottom = '5px';

                const soloButton = document.createElement('div');
                soloButton.style.width = '5px';
                soloButton.style.height = '5px';
                soloButton.style.borderRadius = '50%';
                soloButton.style.backgroundColor = '#333';
                soloButton.style.marginRight = '5px';
                soloButton.style.cursor = 'pointer';
                soloButton.title = `Solo ${layerId}`;
                soloButton.addEventListener('click', () => {
                    window.overtureMap.soloLayer(layerId);
                });

                const layerName = document.createElement('span');
                layerName.textContent = layerId;
                layerName.style.cursor = 'pointer';
                layerName.title = `Toggle ${layerId}`;
                layerName.addEventListener('click', () => {
                    const currentVisibility = window.overtureMap.getMap().getLayoutProperty(layerId, 'visibility');
                    window.overtureMap.toggleLayer(layerId, currentVisibility === 'none');
                });

                layerItem.appendChild(soloButton);
                layerItem.appendChild(layerName);
                legendContainer.appendChild(layerItem);
            });
        }
        
    } catch (error) {
        console.error('Failed to load map:', error);
        mapContainer.innerHTML = '<div style="display: flex; align-items: center; justify-content: center; height: 100%; font-family: sans-serif; color: #e74c3c;">Failed to load map. Please refresh the page.</div>';
    }
});
