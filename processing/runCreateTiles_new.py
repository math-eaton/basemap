#!/usr/bin/env python3
"""
runCreateTiles.py - Convert geospatial data to PMTiles using Tippecanoe

This module handles the conversion of GeoJSON/GeoJSONSeq files to PMTiles
using optimized Tippecanoe settings. Can be used standalone or imported
into other scripts like Jupyter notebooks.

Usage:
    python runCreateTiles.py --extent="20.0,-7.0,26.0,-3.0" --input-dir="/path/to/data"
    
    # From another script:
    from runCreateTiles import process_to_tiles
    process_to_tiles(extent=(20.0, -7.0, 26.0, -3.0), input_dirs=["/path/to/data"])
"""

import os
import subprocess
import fnmatch
import time
from tqdm import tqdm
import sys
import json
import argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

# Set up project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "processing" / "data"
TILE_DIR = PROJECT_ROOT / "processing" / "tiles"
OVERTURE_DATA_DIR = PROJECT_ROOT / "overture" / "data"
PUBLIC_TILES_DIR = PROJECT_ROOT / "public" / "tiles"

def validate_geojson(file_path):
    """Validate and clean GeoJSON files"""
    # Skip validation for GeoJSONSeq files since they're not single JSON objects
    if file_path.suffix == '.geojsonseq':
        return
        
    # Only validate regular GeoJSON files
    with open(file_path, 'r') as f:
        data = json.load(f)

    if 'features' in data:
        data['features'] = [
            feature for feature in data['features']
            if feature.get('geometry') and feature['geometry'].get('coordinates')
        ]

    with open(file_path, 'w') as f:
        json.dump(data, f)

def detect_geometry_type(file_path):
    """Detect the primary geometry type from a GeoJSON or GeoJSONSeq file
    
    Returns: 'Point', 'LineString', 'Polygon', 'MultiPoint', 'MultiLineString', 'MultiPolygon', or 'Mixed'
    """
    try:
        geometry_types = set()
        sample_count = 0
        max_samples = 100  # Sample first 100 features for performance
        
        with open(file_path, 'r') as f:
            # First, try to detect if this is actually a line-delimited JSON file
            # even if it has a .geojson extension
            first_line = f.readline().strip()
            f.seek(0)  # Reset file pointer
            
            # Check if first line is a complete JSON object (feature)
            is_line_delimited = False
            try:
                first_obj = json.loads(first_line)
                if isinstance(first_obj, dict) and first_obj.get('type') == 'Feature':
                    # Check if there's more content after the first line
                    f.readline()  # Skip first line
                    second_line = f.readline().strip()
                    if second_line:
                        try:
                            second_obj = json.loads(second_line)
                            if isinstance(second_obj, dict) and second_obj.get('type') == 'Feature':
                                is_line_delimited = True
                        except json.JSONDecodeError:
                            pass
                f.seek(0)  # Reset file pointer again
            except json.JSONDecodeError:
                pass
            
            if file_path.suffix == '.geojsonseq' or is_line_delimited:
                # Handle GeoJSONSeq files or line-delimited JSON files
                for line in f:
                    line = line.strip()
                    if line and sample_count < max_samples:
                        try:
                            feature = json.loads(line)
                            if 'geometry' in feature and feature['geometry'] and 'type' in feature['geometry']:
                                geom_type = feature['geometry']['type']
                                geometry_types.add(geom_type)
                                sample_count += 1
                        except json.JSONDecodeError:
                            continue
            else:
                # Handle regular GeoJSON files
                try:
                    data = json.load(f)
                    if 'features' in data:
                        for feature in data['features'][:max_samples]:
                            if 'geometry' in feature and feature['geometry'] and 'type' in feature['geometry']:
                                geom_type = feature['geometry']['type']
                                geometry_types.add(geom_type)
                                sample_count += 1
                    elif 'geometry' in data and data['geometry'] and 'type' in data['geometry']:
                        # Single feature GeoJSON
                        geometry_types.add(data['geometry']['type'])
                        sample_count = 1
                except json.JSONDecodeError:
                    return 'Unknown'
        
        if not geometry_types:
            return 'Unknown'
        elif len(geometry_types) == 1:
            return list(geometry_types)[0]
        else:
            return 'Mixed'
            
    except Exception as e:
        print(f"Error detecting geometry type for {file_path}: {e}")
        return 'Unknown'

def get_layer_tippecanoe_settings(layer_name, filename_or_path=None):
    """Get layer-specific tippecanoe settings based on layer name and filename/path
    
    Common options have been consolidated into the base tippecanoe command:
    - --buffer=8 (most layers, higher quality)
    - --no-polygon-splitting (polygon layers)
    - --detect-shared-borders (polygon layers)
    - --drop-smallest (quality optimization)
    - --maximum-tile-bytes=1048576 (1MB standard)
    - --preserve-input-order (consistency)
    - --coalesce-densest-as-needed (most layers)
    - --drop-fraction-as-needed (most layers)
    
    This function now returns only layer-specific options.
    """
    start_time = time.time()
    
    # Handle both Path objects and filename strings
    if filename_or_path:
        if hasattr(filename_or_path, 'name'):  # Path object
            filename = filename_or_path.name
            file_path = filename_or_path
        else:  # String filename
            filename = filename_or_path
            file_path = None
            # Try to find the file in data directories
            for data_dir in [DATA_DIR, OVERTURE_DATA_DIR]:
                potential_path = data_dir / filename
                if potential_path.exists():
                    file_path = potential_path
                    break
    else:
        filename = None
        file_path = None
    
    # Determine layer type from layer name or filename
    layer_type = None
    detection_method = None
    
    # Check layer name first for explicit layer type detection
    if layer_name:
        layer_name_lower = layer_name.lower()
        if layer_name_lower in ['water']:
            layer_type = 'water'
            detection_method = 'layer_name'
        elif layer_name_lower in ['settlement-extents', 'settlementextents']:
            layer_type = 'settlement-extents'
            detection_method = 'layer_name'
        elif layer_name_lower in ['roads']:
            layer_type = 'roads'
            detection_method = 'layer_name'
        elif layer_name_lower in ['places', 'placenames']:
            layer_type = 'places'
            detection_method = 'layer_name'
        elif layer_name_lower in ['land_use', 'land_cover', 'land_residential', 'infrastructure']:
            layer_type = 'base-polygons'
            detection_method = 'layer_name'
    
    # If layer type not determined from layer name, check filename
    if layer_type is None and filename:
        filename_lower = filename.lower()
        # Check for base-polygons first to give land* patterns priority
        # Look for any land-related keywords or specific land layer patterns
        land_keywords = ['land_use', 'land_cover', 'land_residential', 'infrastructure', 'land']
        if any(keyword in filename_lower for keyword in land_keywords):
            layer_type = 'base-polygons'
            detection_method = 'filename_pattern'
        elif 'water' in filename_lower:
            layer_type = 'water'
            detection_method = 'filename_pattern'
        elif 'extents' in filename_lower or 'settlement' in filename_lower:
            layer_type = 'settlement-extents'
            detection_method = 'filename_pattern'
        elif 'roads' in filename_lower:
            layer_type = 'roads'
            detection_method = 'filename_pattern'
        elif 'places' in filename_lower or 'placenames' in filename_lower:
            layer_type = 'places'
            detection_method = 'filename_pattern'
    
    # Track whether geometry detection was needed
    geometry_detection_time = 0
    geometry_type = None
    
    # Return layer-specific tippecanoe flags (common options moved to base command)
    if layer_type == 'water':
        # Optimized for water polygons with enhanced detail at zoom 13+
        settings = [
            '--no-tiny-polygon-reduction',
            '--extend-zooms-if-still-dropping',
            '--maximum-tile-bytes=2097152',  # 2MB for water features (override base)
            '--maximum-zoom=15',         # Extended to match base polygons
        ]
    
    elif layer_type == 'settlement-extents':
        # Settlement extents with special preserved settings
        settings = [
            '--simplification=5',
            '--drop-rate=0.25',
            '--low-detail=11',
            '--full-detail=14',
            '--coalesce-smallest-as-needed',
            '--gamma=0.8',
            '--maximum-zoom=13',
            '--minimum-zoom=6',
            '--cluster-distance=2',
            '--minimum-detail=8'
        ]
    
    elif layer_type == 'roads':
        # Optimized for road lines
        settings = [
            '--no-line-simplification',  # Unique to roads
            '--buffer=16',               # Override base buffer for roads (better quality)
            '--drop-rate=0.05',          # Very conservative for roads
            '--drop-smallest',
            '--simplification=5',
            '--minimum-zoom=7',
            '--extend-zooms-if-still-dropping',
            '--coalesce-smallest-as-needed',
            '--full-detail=13',
            '--minimum-detail=10'
        ]
    
    elif layer_type == 'places':
        # Optimized for point features (minimal settings needed)
        settings = [
            '--cluster-distance=10',     # Reduced for better point preservation
            '--drop-rate=0.0',          # NO dropping for point features
            '--no-feature-limit',       # Ensure all points are preserved
            '--extend-zooms-if-still-dropping',  # Extend zooms to prevent dropping
            '--maximum-zoom=16',        # Ensure points visible at highest zooms
        ]
    
    elif layer_type == 'base-polygons':
        # Optimized for base polygon layers (land_use, land_cover, etc.)
        settings = [
            '--extend-zooms-if-still-dropping-maximum=16',
            '--drop-rate=0.1',
            '--coalesce-densest-as-needed',
            '--minimum-zoom=8',
            '--maximum-zoom=15',
        ]
    
    else:
        # Default settings based on geometry type detection
        detection_method = 'geometry_detection'
        if file_path and file_path.exists():
            geom_start_time = time.time()
            geometry_type = detect_geometry_type(file_path)
            geometry_detection_time = time.time() - geom_start_time
            print(f"  Detected geometry type: {geometry_type} for {filename} ({geometry_detection_time:.3f}s)")
        else:
            geometry_type = 'Unknown'
            print(f"  Could not detect geometry type for {filename}, using polygon defaults")
        
        # Return geometry-specific settings
        if geometry_type == 'Point':
            # Optimized for point features
            settings = [
                '--cluster-distance=35',     # Point clustering for better display
                '--drop-rate=0.05',         # Very conservative for points
                '--low-detail=8',           # Earlier detail for points visibility
                '--full-detail=11',         # Earlier full detail for points
                '--coalesce-smallest-as-needed',
                '--extend-zooms-if-still-dropping',
                '--gamma=0.3',              # Less aggressive for point density
                '--maximum-zoom=15',
                '--minimum-zoom=6',         # Points visible at lower zooms
                '--simplification=1',       # Minimal simplification for points
            ]
        
        elif geometry_type == 'LineString':
            # Optimized for line features (roads, infrastructure, etc.)
            settings = [
                '--no-line-simplification', # Preserve line geometry
                '--drop-rate=0.08',         # Conservative for linear features
                '--low-detail=9',           # Good detail preservation
                '--full-detail=12',         
                '--coalesce-smallest-as-needed',
                '--extend-zooms-if-still-dropping',
                '--gamma=0.4',              # Moderate density reduction
                '--maximum-zoom=15',
                '--minimum-zoom=7',         # Lines visible at medium zooms
                '--simplification=3',       # Moderate simplification for lines
                '--buffer=12',              # Higher buffer for line features
            ]
        
        elif geometry_type == 'Polygon':
            # Optimized for polygon features (default polygon settings)
            settings = [
                '--simplification=5',        # Moderate simplification for polygons
                '--drop-rate=0.1',          # Conservative dropping
                '--low-detail=10',          # Standard detail start
                '--full-detail=13',         # Good full detail
                '--coalesce-smallest-as-needed',
                '--extend-zooms-if-still-dropping',
                '--gamma=0.5',              # Balanced density reduction
                '--maximum-zoom=15',
                '--minimum-zoom=8',         # Polygons at higher zooms
            ]
        
        else:
            # Mixed or Unknown geometry types - use conservative polygon defaults
            settings = [
                '--simplification=19',        # Conservative simplification
                '--drop-rate=0.08',         # Very conservative dropping
                '--low-detail=9',           # Early detail preservation
                '--full-detail=15',         
                '--coalesce-smallest-as-needed',
                '--extend-zooms-if-still-dropping',
                '--maximum-zoom=15',
                '--minimum-zoom=7',
            ]
    
    # Log performance and decision metrics
    total_time = time.time() - start_time
    
    # Only log detailed metrics if debugging is enabled (check for debug flag or verbose mode)
    if hasattr(sys, 'argv') and ('--debug' in sys.argv or '--verbose' in sys.argv):
        identifier = layer_name if layer_name else (filename if filename else 'unknown')
        print(f"  Settings selection for '{identifier}':")
        print(f"    Method: {detection_method}")
        print(f"    Layer type: {layer_type}")
        print(f"    Geometry type: {geometry_type}")
        print(f"    Total time: {total_time:.3f}s")
        print(f"    Geometry detection time: {geometry_detection_time:.3f}s")
        print(f"    Settings count: {len(settings)}")
    
    return settings

def get_tippecanoe_command(input_path, tile_path, layer_name, extent=None):
    """Generate optimized tippecanoe command for converting GeoJSON to PMTiles
    
    Args:
        input_path (Path): Path to input GeoJSON/GeoJSONSeq file
        tile_path (Path): Path for output PMTiles file
        layer_name (str): Name for the tile layer
        extent (tuple): Optional bounding box (xmin, ymin, xmax, ymax)
    
    Returns:
        list: Command arguments for subprocess.run()
    """
    
    # Base tippecanoe command with common optimizations
    base_cmd = [
        'tippecanoe',
        '-fo', str(tile_path),  # Force overwrite output to PMTiles
        '-l', layer_name,       # Layer name
        '--buffer=8',           # Higher quality buffer (most layers)
        '--drop-smallest',      # Quality optimization
        '--maximum-tile-bytes=1048576',  # 1MB standard tile size
        '--preserve-input-order',        # Consistency
        '--coalesce-densest-as-needed',  # Most layers
        '--drop-fraction-as-needed',     # Most layers
        '-P',                   # Parallel processing
    ]
    
    # Add extent clipping if provided
    if extent:
        xmin, ymin, xmax, ymax = extent
        base_cmd.extend(['--clip-bounding-box', f"{xmin},{ymin},{xmax},{ymax}"])
    
    # Add polygon-specific options for non-point layers
    # We'll detect this based on the layer settings
    layer_settings = get_layer_tippecanoe_settings(layer_name, input_path)
    
    # Check if this appears to be a polygon layer (add polygon-specific options)
    if not any('cluster-distance' in setting for setting in layer_settings):
        # This is likely a polygon layer, add polygon-specific optimizations
        base_cmd.extend([
            '--no-polygon-splitting',
            '--detect-shared-borders',
        ])
    
    # Add layer-specific settings
    base_cmd.extend(layer_settings)
    
    # Add input file at the end
    base_cmd.append(str(input_path))
    
    return base_cmd

def process_single_file(file_path, extent=None, output_dir=None):
    """Process a single file into PMTiles - designed for parallel execution"""
    try:
        layer_name = file_path.stem.replace('_', '-')  # Use filename as layer name
        
        # Determine output directory
        if output_dir:
            tile_dir = Path(output_dir)
        else:
            tile_dir = TILE_DIR
        
        tile_dir.mkdir(parents=True, exist_ok=True)
        tile_path = tile_dir / f"{file_path.stem}.pmtiles"
        
        if not file_path.exists():
            return {"success": False, "message": f"File does not exist: {file_path}"}
        
        # Validate GeoJSON structure
        validate_geojson(file_path)
        
        # Get optimized tippecanoe settings based on file type
        cmd = get_tippecanoe_command(file_path, tile_path, layer_name, extent)
        
        # Execute tippecanoe
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        return {
            "success": True,
            "message": f"Tiles generated successfully: {tile_path.name}",
            "output_file": tile_path,
            "layer_name": layer_name
        }
        
    except subprocess.CalledProcessError as e:
        return {
            "success": False,
            "message": f"Tippecanoe error: {e.stderr if e.stderr else str(e)}",
            "command": ' '.join(cmd) if 'cmd' in locals() else 'unknown'
        }
    except Exception as e:
        return {"success": False, "message": f"Error: {str(e)}"}

def process_to_tiles(extent=None, input_dirs=None, filter_pattern=None, 
                    output_dir=None, parallel=True, verbose=True):
    """Process GeoJSON/GeoJSONSeq files into PMTiles
    
    Args:
        extent (tuple): Bounding box as (xmin, ymin, xmax, ymax)
        input_dirs (list): List of directories to search for input files
        filter_pattern (str): Only process files matching this pattern
        output_dir (str): Directory to write output PMTiles (default: TILE_DIR)
        parallel (bool): Use parallel processing (default: True)
        verbose (bool): Show progress information (default: True)
    
    Returns:
        dict: Results including processed files and any errors
    """
    if verbose:
        print("=== PROCESSING TO TILES ===")
    
    # Default input directories
    if input_dirs is None:
        input_dirs = [DATA_DIR, OVERTURE_DATA_DIR]
    else:
        input_dirs = [Path(d) for d in input_dirs]
    
    # Ensure directories exist
    for data_dir in input_dirs:
        Path(data_dir).mkdir(parents=True, exist_ok=True)
    
    if output_dir:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
    else:
        TILE_DIR.mkdir(parents=True, exist_ok=True)
    
    # Find all GeoJSON/GeoJSONSeq files in data directories
    geojson_files = []
    
    # Search in all input directories
    for data_dir in input_dirs:
        data_dir = Path(data_dir)
        if data_dir.exists():
            for pattern in ['*.geojson', '*.geojsonseq']:
                geojson_files.extend(data_dir.glob(pattern))
    
    # Apply filter if provided
    if filter_pattern:
        filtered_files = []
        for f in geojson_files:
            if fnmatch.fnmatch(f.name, filter_pattern):
                filtered_files.append(f)
        geojson_files = filtered_files
    
    if not geojson_files:
        message = "No GeoJSON/GeoJSONSeq files found"
        if filter_pattern:
            message += f" matching pattern '{filter_pattern}'"
        print(message)
        return {"success": False, "message": message, "processed_files": [], "errors": []}
    
    if verbose:
        print(f"Found {len(geojson_files)} files to process:")
        for f in geojson_files:
            print(f"  {f.name}")
    
    results = {
        "success": True,
        "processed_files": [],
        "errors": [],
        "total_files": len(geojson_files)
    }
    
    # Process files
    if parallel and len(geojson_files) > 1:
        # Parallel processing
        with ProcessPoolExecutor(max_workers=min(4, len(geojson_files))) as executor:
            # Submit all jobs
            future_to_file = {
                executor.submit(process_single_file, geojson_file, extent, output_dir): geojson_file
                for geojson_file in geojson_files
            }
            
            # Process results with progress bar
            if verbose:
                progress_bar = tqdm(total=len(geojson_files), desc="Processing files", unit="file")
            
            for future in as_completed(future_to_file):
                geojson_file = future_to_file[future]
                try:
                    result = future.result()
                    if result["success"]:
                        results["processed_files"].append({
                            "input_file": geojson_file.name,
                            "output_file": result.get("output_file"),
                            "layer_name": result.get("layer_name")
                        })
                        if verbose:
                            tqdm.write(f"✓ {geojson_file.name} -> {result.get('output_file', 'unknown')}")
                    else:
                        results["errors"].append({
                            "file": geojson_file.name,
                            "error": result["message"]
                        })
                        if verbose:
                            tqdm.write(f"✗ {geojson_file.name}: {result['message']}")
                except Exception as e:
                    error_msg = f"Unexpected error: {str(e)}"
                    results["errors"].append({
                        "file": geojson_file.name,
                        "error": error_msg
                    })
                    if verbose:
                        tqdm.write(f"✗ {geojson_file.name}: {error_msg}")
                
                if verbose:
                    progress_bar.update(1)
            
            if verbose:
                progress_bar.close()
    
    else:
        # Sequential processing
        if verbose:
            progress_bar = tqdm(geojson_files, desc="Processing files", unit="file")
        else:
            progress_bar = geojson_files
        
        for geojson_file in progress_bar:
            result = process_single_file(geojson_file, extent, output_dir)
            
            if result["success"]:
                results["processed_files"].append({
                    "input_file": geojson_file.name,
                    "output_file": result.get("output_file"),
                    "layer_name": result.get("layer_name")
                })
                if verbose:
                    tqdm.write(f"✓ {geojson_file.name} -> {result.get('output_file', 'unknown')}")
            else:
                results["errors"].append({
                    "file": geojson_file.name,
                    "error": result["message"]
                })
                if verbose:
                    tqdm.write(f"✗ {geojson_file.name}: {result['message']}")
    
    # Set overall success status
    if results["errors"]:
        results["success"] = False
    
    if verbose:
        print(f"\n=== TILE PROCESSING COMPLETE ===")
        print(f"Processed: {len(results['processed_files'])}/{results['total_files']} files")
        if results["errors"]:
            print(f"Errors: {len(results['errors'])}")
    
    return results

def create_tilejson(tile_dir=None, extent=None, output_file=None):
    """Generate TileJSON for MapLibre integration
    
    Args:
        tile_dir (str|Path): Directory containing PMTiles files
        extent (tuple): Bounding box as (xmin, ymin, xmax, ymax)
        output_file (str|Path): Output TileJSON file path
    
    Returns:
        dict: TileJSON structure
    """
    if tile_dir is None:
        tile_dir = TILE_DIR
    else:
        tile_dir = Path(tile_dir)
    
    if extent is None:
        # Use a default extent if none provided
        extent = (-180, -85, 180, 85)
    
    if output_file is None:
        output_file = tile_dir / "tilejson.json"
    else:
        output_file = Path(output_file)
    
    xmin, ymin, xmax, ymax = extent
    
    # Base TileJSON structure
    tilejson = {
        "tilejson": "3.0.0",
        "name": "Basemap Tiles",
        "minzoom": 0,
        "maxzoom": 16,
        "bounds": [xmin, ymin, xmax, ymax],
        "tiles": [],
        "vector_layers": []
    }
    
    # Find all PMTiles files
    pmtiles_files = list(tile_dir.glob("*.pmtiles"))
    
    # Add each PMTiles file as a tile source
    for pmtiles_file in sorted(pmtiles_files):
        # Create relative URL
        tile_url = f"pmtiles://{pmtiles_file.name}"
        tilejson["tiles"].append(tile_url)
        
        # Add vector layer info
        layer_name = pmtiles_file.stem
        vector_layer = {
            "id": layer_name,
            "description": f"Layer: {layer_name}",
            "fields": {"id": "String", "name": "String"}  # Generic fields
        }
        tilejson["vector_layers"].append(vector_layer)
    
    # Write TileJSON file
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w') as f:
        json.dump(tilejson, f, indent=2)
    
    print(f"TileJSON created: {output_file}")
    print(f"Found {len(pmtiles_files)} PMTiles files")
    
    return tilejson

def main():
    """Main entry point for command line usage"""
    parser = argparse.ArgumentParser(
        description='Convert geospatial data to PMTiles using Tippecanoe',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument('--extent', 
                        help='Extent as "xmin,ymin,xmax,ymax" in WGS84 coordinates')
    parser.add_argument('--input-dir', action='append',
                        help='Input directory to search for GeoJSON files (can be used multiple times)')
    parser.add_argument('--output-dir',
                        help='Output directory for PMTiles files')
    parser.add_argument('--filter',
                        help='Only process files matching this pattern (e.g., "roads*" or "places.geojson")')
    parser.add_argument('--no-parallel', action='store_true',
                        help='Disable parallel processing')
    parser.add_argument('--create-tilejson', action='store_true',
                        help='Create TileJSON file after processing')
    parser.add_argument('--verbose', action='store_true', default=True,
                        help='Show detailed progress information')
    
    args = parser.parse_args()
    
    # Parse extent if provided
    extent = None
    if args.extent:
        try:
            extent_parts = args.extent.split(',')
            if len(extent_parts) != 4:
                raise ValueError("Extent must have 4 values")
            extent = tuple(float(x) for x in extent_parts)
        except ValueError as e:
            print(f"Error parsing extent: {e}")
            print("Extent format: xmin,ymin,xmax,ymax")
            sys.exit(1)
    
    # Process tiles
    results = process_to_tiles(
        extent=extent,
        input_dirs=args.input_dir,
        filter_pattern=args.filter,
        output_dir=args.output_dir,
        parallel=not args.no_parallel,
        verbose=args.verbose
    )
    
    # Create TileJSON if requested
    if args.create_tilejson:
        create_tilejson(
            tile_dir=args.output_dir or TILE_DIR,
            extent=extent
        )
    
    # Report results
    if results["success"]:
        print(f"\nSuccessfully processed {len(results['processed_files'])} files")
    else:
        print(f"\nProcessing completed with {len(results['errors'])} errors:")
        for error in results["errors"]:
            print(f"  - {error['file']}: {error['error']}")
        sys.exit(1)

if __name__ == "__main__":
    main()
