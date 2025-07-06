import { defineConfig } from 'vite'

export default defineConfig({
  base: './', // For GitHub Pages deployment
  build: {
    outDir: 'dist',
    assetsDir: 'assets',
    sourcemap: true,
    rollupOptions: {
      output: {
        manualChunks: {
          // Separate MapLibre and related mapping libraries
          'maplibre': ['maplibre-gl'],
          'contour': ['maplibre-contour'],
          'pmtiles': ['pmtiles'],
          // Keep application code separate
          'app': ['./src/js/basemap.js']
        }
      }
    },
    // Increase chunk size warning limit for mapping applications
    chunkSizeWarningLimit: 1000
  },
  server: {
    port: 3000,
    open: true
  },
  optimizeDeps: {
    include: ['maplibre-gl', 'pmtiles'],
    exclude: ['maplibre-contour']
  }
})
