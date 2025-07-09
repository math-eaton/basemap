# Overture Basemap

A responsive, modern basemap application built with MapLibre GL JS and Overture Maps data.

## Features

- **Responsive Design**: Optimized for both mobile and desktop experiences
- **Mobile-First Touch Controls**: Enhanced touch interactions for mobile devices
- **Custom Attribution**: Includes Overture Maps, OpenStreetMap, and USGS references
- **Camera Bounds Clamping**: Optional feature to restrict camera movement to defined bounds
- **Contour Lines**: Optional topographic contour overlays
- **Hillshade**: Terrain visualization with hillshade rendering

## Configuration Options

The map can be configured with the following options:

```javascript
const map = new OvertureMap('map-container', {
    // Geographic bounds for St. Lawrence County
    bounds: [
        [-75.5, 44.0], // Southwest coordinates [lng, lat]
        [-74.5, 45.0]  // Northeast coordinates [lng, lat]
    ],
    center: [-74.986763650502, 44.66997929549087],
    zoom: 13,
    minZoom: 11,
    maxZoom: 16,
    showTileBoundaries: false,
    clampToBounds: false // Set to true to restrict camera movement to the defined bounds
});
```

### Camera Bounds Clamping

When `clampToBounds` is set to `true`, the camera will be restricted to stay within the defined bounds. This prevents users from panning outside the area of interest and ensures they always see relevant data.

## Mobile Optimizations

- Touch-optimized camera controls with enhanced deceleration
- Disabled keyboard controls on mobile devices
- Performance optimizations for better frame rates
- Responsive attribution control with compact mode
- Touch-friendly interaction patterns

## Desktop Experience

- Uses MapLibre GL JS default camera controls for smooth interactions
- Full-featured attribution display
- Optimized for mouse and keyboard navigation

## Development

```bash
# Install dependencies
npm install

# Start development server
npm run dev

# Build for production
npm run build
```

## Data Sources

- **Base Map Data**: [Overture Maps](https://overturemaps.org/)
- **Background Map**: [OpenStreetMap](https://www.openstreetmap.org/)
- **Elevation Data**: [USGS](https://www.usgs.gov/)
