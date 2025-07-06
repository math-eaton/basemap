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
        
        // Initialize the Overture map
        const overtureMap = new OvertureMap('map', {
            // override default options here
            // bounds: [[-75.5, 44.0], [-74.5, 45.0]],
            // zoom: 14,
            // minZoom: 11,
            // maxZoom: 16
        });
        
        // Make overtureMap available globally for debugging
        window.overtureMap = overtureMap;
        
        console.log('Overture map initialization complete');
        
    } catch (error) {
        console.error('Failed to load map:', error);
        mapContainer.innerHTML = '<div style="display: flex; align-items: center; justify-content: center; height: 100%; font-family: sans-serif; color: #e74c3c;">Failed to load map. Please refresh the page.</div>';
    }
});
