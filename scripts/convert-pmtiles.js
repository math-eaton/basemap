#!/usr/bin/env node

/**
 * Script to convert PMTiles to traditional vector tile directories
 * This provides a fallback for hosting environments that don't support HTTP range requests
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

console.log('🔄 Converting PMTiles to Vector Tile Directories...\n');

// Check if pmtiles CLI is installed
try {
    execSync('pmtiles --version', { stdio: 'ignore' });
} catch (error) {
    console.error('❌ pmtiles CLI not found. Install with: npm install -g pmtiles');
    process.exit(1);
}

const tilesDir = path.join(__dirname, '../public/tiles');
const vectorTilesDir = path.join(__dirname, '../public/vector-tiles');

// Create vector-tiles directory
if (!fs.existsSync(vectorTilesDir)) {
    fs.mkdirSync(vectorTilesDir, { recursive: true });
}

// Find all PMTiles files
const pmtilesFiles = fs.readdirSync(tilesDir).filter(file => file.endsWith('.pmtiles'));

if (pmtilesFiles.length === 0) {
    console.log('ℹ️  No PMTiles files found in public/tiles/');
    process.exit(0);
}

console.log(`Found ${pmtilesFiles.length} PMTiles files to convert:\n`);

for (const pmtileFile of pmtilesFiles) {
    const baseName = path.basename(pmtileFile, '.pmtiles');
    const inputPath = path.join(tilesDir, pmtileFile);
    const outputDir = path.join(vectorTilesDir, baseName);
    
    console.log(`📦 Converting ${pmtileFile}...`);
    
    try {
        // Create output directory
        if (!fs.existsSync(outputDir)) {
            fs.mkdirSync(outputDir, { recursive: true });
        }
        
        // Extract tiles using pmtiles CLI
        execSync(`pmtiles extract "${inputPath}" "${outputDir}"`, { stdio: 'inherit' });
        console.log(`✅ ${baseName} converted successfully`);
        
    } catch (error) {
        console.error(`❌ Failed to convert ${pmtileFile}:`, error.message);
    }
    
    console.log('');
}

// Create vector tile style file
console.log('🎨 Creating vector tile style file...');
try {
    const stylePath = path.join(__dirname, '../src/styles/cartography.json');
    const style = JSON.parse(fs.readFileSync(stylePath, 'utf8'));
    
    // Update sources to use traditional vector tiles
    for (const [sourceId, source] of Object.entries(style.sources)) {
        if (source.type === 'vector' && source.url && source.url.includes('pmtiles://')) {
            const tileName = sourceId.replace('-tiles', '');
            source.tiles = [`./vector-tiles/${tileName}/{z}/{x}/{y}.pbf`];
            source.minzoom = 0;
            source.maxzoom = 14;
            delete source.url;
        }
    }
    
    const vectorStylePath = path.join(__dirname, '../public/cartography-vector.json');
    fs.writeFileSync(vectorStylePath, JSON.stringify(style, null, 2));
    console.log('✅ cartography-vector.json created');
    
} catch (error) {
    console.error('❌ Failed to create vector style:', error.message);
}

console.log('\n🎉 Conversion complete!');
console.log('\nTo use vector tiles instead of PMTiles:');
console.log('1. Set useVectorTiles: true in your map options');
console.log('2. Or manually load cartography-vector.json instead of cartography.json');
console.log('\nNote: Vector tiles will take more storage space but work reliably on GitHub Pages.');
