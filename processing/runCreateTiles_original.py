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
    
    def replace_url(match, url_type="s3"):
        original_url = match.group(1)
        theme = match.group(2)
        data_type = match.group(3) if len(match.groups()) >= 3 else "unknown"
        additional_params = match.group(4) if len(match.groups()) >= 4 else ""
        
        # Get optimized paths for this URL
        optimized_paths = optimize_parquet_paths(original_url, quadkeys, url_type)
        
        # Create array of paths for read_parquet
        paths_array = "[" + ", ".join(f"'{path}'" for path in optimized_paths) + "]"
        
        # Reconstruct the read_parquet call with the optimized paths
        result = f"read_parquet({paths_array}{additional_params})"
        
        # Log the optimization
        print(f"  Optimized {theme}/{data_type} ({url_type.upper()}): {len(optimized_paths)} partitions (was: full dataset)")
        
        return result
    
    def replace_s3_url(match):
        return replace_url(match, "s3")
    
    def replace_azure_url(match):
        return replace_url(match, "azure")
    
    def replace_s3_places_url(match):
        # Special handler for places URLs with /*/* structure
        original_url = match.group(1)
        theme = match.group(2)
        additional_params = match.group(3)
        
        # For places, we need to handle the /*/* structure
        optimized_paths = optimize_parquet_paths(original_url + "/*", quadkeys, "s3")
        
        # Create array of paths for read_parquet
        paths_array = "[" + ", ".join(f"'{path}'" for path in optimized_paths) + "]"
        
        # Reconstruct the read_parquet call with the optimized paths
        result = f"read_parquet({paths_array}{additional_params})"
        
        # Log the optimization
        print(f"  Optimized {theme}/all (S3): {len(optimized_paths)} partitions (was: full dataset)")
        
        return result
    
    # Apply the optimization to all URL types
    optimized_sql = re.sub(s3_pattern, replace_s3_url, sql_content)
    optimized_sql = re.sub(azure_pattern, replace_azure_url, optimized_sql)
    optimized_sql = re.sub(s3_places_pattern, replace_s3_places_url, optimized_sql)
    
    return optimized_sql

def snap_to_tile_bounds(extent, zoom=8):
    """Snap extent to align with slippy tile boundaries to prevent rendering artifacts"""
    xmin, ymin, xmax, ymax = extent
    tiles = list(mercantile.tiles(xmin, ymin, xmax, ymax, zoom))
    if not tiles:
        return extent
    
    snapped = mercantile.bounds(tiles[0])
    for t in tiles[1:]:
        b = mercantile.bounds(t)
        snapped = mercantile.LngLatBbox(
            min(snapped.west, b.west),
            min(snapped.south, b.south),
            max(snapped.east, b.east),
            max(snapped.north, b.north),
        )
    return (snapped.west, snapped.south, snapped.east, snapped.north)

# extent parameters for New York State
# extent_xmin = -79.76259
# extent_xmax = -71.85621
# extent_ymin = 40.49612
# extent_ymax = 45.01585

# extent parameters for st lawrence county, ny
# extent_xmin = -75.5
# extent_xmax = -74.5
# extent_ymin = 44.0
# extent_ymax = 45.0

# extent parameters for Kinshasa Province, DRC
# extent_xmin = 15.0
# extent_xmax = 16.0
# extent_ymin = -4.5
# extent_ymax = -3.5

# extent parameters for Kasai-Oriental Province, DRC
raw_extent = (20.0, -7.0, 26.0, -3.0)  # (xmin, ymin, xmax, ymax)

# testing extent - mbuji-mayi, drc
# raw_extent = (23.4, -6.2, 23.8, -5.8)  # (xmin, ymin, xmax, ymax)


# Snap to tile boundaries to prevent rendering artifacts
snapped_extent = snap_to_tile_bounds(raw_extent, zoom=8)
extent_xmin, extent_ymin, extent_xmax, extent_ymax = snapped_extent

print(f"Raw extent: {raw_extent}")
print(f"Snapped extent: {snapped_extent}")

# Buffer for data download to ensure complete features at edges
# Optimized: reduced from 1° to 0.2° (80% reduction in download area)
buffer_degrees = 0.2  # ~22km buffer (was ~111km)

# Buffered extent for data download
buffered_xmin = extent_xmin - buffer_degrees
buffered_xmax = extent_xmax + buffer_degrees
buffered_ymin = extent_ymin - buffer_degrees
buffered_ymax = extent_ymax + buffer_degrees


def download_source_data():
    """Download and process source data from Overture Maps
    
    Uses a buffered extent to ensure complete features at map boundaries.
    The buffer helps prevent edge clipping when generating tiles.
    """
    print("=== DOWNLOADING SOURCE DATA ===")
    print(f"Map extent: {extent_xmin}, {extent_ymin} to {extent_xmax}, {extent_ymax}")
    print(f"Download extent (buffered): {buffered_xmin}, {buffered_ymin} to {buffered_xmax}, {buffered_ymax}")
    print(f"Buffer: {buffer_degrees} degrees (~{buffer_degrees * 111:.1f}km)")
    print()
    
    # Ensure directories exist
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OVERTURE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Read the SQL template file
    template_path = Path(__file__).parent / 'tileQueries.template'
    with open(template_path, 'r') as file:
        template_content = file.read()

    # Replace template variables with actual paths
    sql_content = template_content.replace('{{data_dir}}', str(DATA_DIR))
    sql_content = sql_content.replace('{{overture_data_dir}}', str(OVERTURE_DATA_DIR))
    
    # Replace the extent variables with buffered extent
    sql_content = sql_content.replace('$extent_xmin', str(buffered_xmin))
    sql_content = sql_content.replace('$extent_xmax', str(buffered_xmax))
    sql_content = sql_content.replace('$extent_ymin', str(buffered_ymin))
    sql_content = sql_content.replace('$extent_ymax', str(buffered_ymax))

    # OPTIMIZATION: Get QuadKeys for the buffered extent to filter parquet files
    quadkeys = get_quadkeys_for_extent(buffered_xmin, buffered_ymin, buffered_xmax, buffered_ymax, zoom=6)
    total_possible_quadkeys = 4**6  # 6 zoom levels, 4 quadrants each = 4096 total
    efficiency_percent = (len(quadkeys) / total_possible_quadkeys) * 100
    # print(f"QuadKey optimization: {len(quadkeys)}/{total_possible_quadkeys} partitions to read ({efficiency_percent:.1f}% of global data)")
    # print(f"Estimated data reduction: {100 - efficiency_percent:.1f}% less data to download")
    # print(f"QuadKeys: {quadkeys[:10]}{'...' if len(quadkeys) > 10 else ''}")
    
    # Apply QuadKey filtering to S3 URLs in the SQL
    # Temporarily disable QuadKey optimization
    # sql_content = optimize_sql_with_quadkeys(sql_content, quadkeys)

    # Split the SQL content into sections based on '-- breakpoint'
    sql_sections = sql_content.split('-- breakpoint')

    # Connect to DuckDB
    conn = duckdb.connect()

    # Create a progress bar for the overall process
    with tqdm(total=len([s for s in sql_sections if s.strip() and not s.strip().startswith('SET extent_')]), 
              desc="Overall progress", unit="section", position=0, leave=True) as pbar:
        
        # Execute each section
        for i, section in enumerate(sql_sections):
            section = section.strip()
            if section and not section.startswith('SET extent_'):  # Skip empty sections and SET commands
                # Extract URL and data type from the section
                url_info = get_db_url(section)
                if url_info:
                    desc = f"Section {i + 1}: {url_info['description']}"
                    tqdm.write(f"Executing {desc}")
                    tqdm.write(f"  -> Querying: {url_info['url']}")
                    tqdm.write(f"  -> Output: {url_info['output_file']}")
                else:
                    desc = f"Section {i + 1}"
                    tqdm.write(f"Executing {desc}...")
                
                try:
                    # Create a callback for progress updates during query execution
                    class ProgressTracker:
                        def __init__(self):
                            self.progress_bar = None
                            self.last_progress = 0
                            self.current_step = 0
                            self.total_steps = 100  # Estimate
                        
                        def progress(self, current, total=None):
                            if total is not None and total > 0:
                                self.total_steps = total
                            
                            # Initialize progress bar if not already done
                            if self.progress_bar is None:
                                self.progress_bar = tqdm(
                                    total=self.total_steps, 
                                    desc=f"  {desc}", 
                                    unit="op", 
                                    position=1, 
                                    leave=False
                                )
                            
                            # Update progress bar only for significant changes
                            if current > self.last_progress:
                                self.progress_bar.update(current - self.last_progress)
                                self.last_progress = current
                            
                            # Complete the progress bar when done
                            if current >= self.total_steps:
                                if self.progress_bar:
                                    self.progress_bar.close()
                                    self.progress_bar = None
                    
                    # Create a tracker for this query
                    tracker = ProgressTracker()
                    
                    # Execute the query
                    conn.execute(section)
                    
                    # Make sure progress bar is closed
                    if tracker.progress_bar:
                        tracker.progress_bar.close()
                    
                    tqdm.write(f"  SUCCESS: Section {i + 1} executed successfully.")
                except Exception as e:
                    tqdm.write(f"  ERROR: Error executing section {i + 1}: {e}")
                    tqdm.write(f"  Section content: {section[:200]}...")
                
                # Update the main progress bar
                pbar.update(1)

    # Close the connection
    conn.close()
    tqdm.write("=== SOURCE DATA DOWNLOAD COMPLETE ===\n")

def process_to_tiles(filter_pattern=None):
    """Process GeoJSON/GeoJSONSeq files into individual zoom-level PMTiles"""
    print("=== PROCESSING TO TILES ===")
    
    # Ensure directories exist
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OVERTURE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    TILE_DIR.mkdir(parents=True, exist_ok=True)
    
    # Find all GeoJSON/GeoJSONSeq files in both data directories
    geojson_files = []
    
    # Search in both data directories
    for data_dir in [DATA_DIR, OVERTURE_DATA_DIR]:
        if data_dir.exists():
            found_files = [
                f for f in data_dir.iterdir()
                if (f.suffix in ['.geojson', '.geojsonseq']) and not f.name.endswith('.pmtiles')
            ]
            geojson_files.extend(found_files)
    
    # Apply filter if provided
    if filter_pattern:
        geojson_files = [f for f in geojson_files if fnmatch.fnmatch(f.name, filter_pattern)]
    
    if not geojson_files:
        print("No GeoJSON/GeoJSONSeq files found. Run download_source_data() first.")
        return
    
    print(f"Found {len(geojson_files)} files to process:")
    for f in geojson_files:
        print(f"  - {f.name}")
    
    # Process each file individually
    for geojson_file in geojson_files:
        if not geojson_file.exists():
            print(f"Warning: {geojson_file} does not exist, skipping...")
            continue
        
        print(f"\n--- Processing {geojson_file.name} ---")
        
        # Determine layer name from filename (remove extensions)
        layer_name = geojson_file.stem
        if layer_name.endswith('.geojsonseq'):
            layer_name = layer_name[:-12]  # Remove .geojsonseq
        
        # Create layer directory
        layer_dir = TILE_DIR / layer_name
        layer_dir.mkdir(parents=True, exist_ok=True)
        
        # Special handling for buildings with multi-LOD
        if 'building' in geojson_file.name:
            create_building_tiles_individual(geojson_file, layer_dir, layer_name)
        else:
            # Process as individual zoom-level PMTiles
            process_individual_layer(geojson_file, layer_dir, layer_name)
    
    print("=== TILE PROCESSING COMPLETE ===\n")

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

def process_single_file(file_path):
    """Process a single file into PMTiles - designed for parallel execution"""
    try:
        layer_name = 'layer'
        tile_path = TILE_DIR / f"{file_path.stem}.pmtiles"
        
        if not file_path.exists():
            return {"success": False, "message": f"File does not exist: {file_path}"}
            
        # Get optimized tippecanoe settings based on file type
        cmd = get_tippecanoe_command(file_path, tile_path, layer_name)
        
        # Execute tippecanoe
        subprocess.run(cmd, check=True)
        
        return {"success": True, "message": f"Tiles generated successfully"}
        
    except subprocess.CalledProcessError as e:
        return {"success": False, "message": f"Tippecanoe error: {e.stderr.decode() if e.stderr else str(e)}"}
    except Exception as e:
        return {"success": False, "message": f"Error: {str(e)}"}

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
                except json.JSONDecodeError:
                    return 'Unknown'
        
        # Normalize geometry types to base types
        normalized_types = set()
        for geom_type in geometry_types:
            if geom_type in ['Point', 'MultiPoint']:
                normalized_types.add('Point')
            elif geom_type in ['LineString', 'MultiLineString']:
                normalized_types.add('LineString')
            elif geom_type in ['Polygon', 'MultiPolygon']:
                normalized_types.add('Polygon')
            else:
                normalized_types.add(geom_type)
        
        # Return the primary geometry type
        if len(normalized_types) == 1:
            return list(normalized_types)[0]
        elif len(normalized_types) > 1:
            return 'Mixed'
        else:
            return 'Unknown'
            
    except Exception as e:
        print(f"Warning: Could not detect geometry type for {file_path}: {e}")
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
    import time
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
            # '--simplification=2',        # Reduced for better coastline detail (better than default)
            # '--low-detail=12',           # Earlier detail start
            # '--full-detail=13',        
            '--no-tiny-polygon-reduction',
            # '--no-feature-limit',
            '--extend-zooms-if-still-dropping',
            '--maximum-tile-bytes=2097152',  # 2MB for water features (override base)
            '--maximum-zoom=15',         # Extended to match base polygons
            # '--gamma=0.9',               # Less aggressive for water bodies
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
            # '--low-detail=11',
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
            # '--no-tiny-polygon-reduction',
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
                # '--no-tiny-polygon-reduction',  # Preserve small polygons
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
                # '--gamma=0.4',              # Moderate density reduction
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
    
def get_tippecanoe_command(input_path, tile_path, layer_name):
    """Get tippecanoe command based on file type - simplified approach"""
    filename = Path(input_path).name.lower()
    
    # Base command
    base_cmd = [
        'tippecanoe',
        '-fo', str(tile_path),
        '-l', layer_name,
        '--clip-bounding-box', f"{extent_xmin},{extent_ymin},{extent_xmax},{extent_ymax}",
        '-P',
        str(input_path)
    ]
    
    # Layer-specific settings based on filename patterns
    if 'water' in filename:
        # Water features - preserve polygon topology
        return base_cmd + [
            '-zg',
            '--detect-shared-borders',
            '--no-tiny-polygon-reduction',
            '--low-detail=13',
            '--full-detail=15',
            '--no-feature-limit',
            '--buffer=64',
            '--drop-fraction-as-needed',
            '--preserve-input-order',
            '--coalesce-densest-as-needed',
            '--extend-zooms-if-still-dropping',
            '--maximum-tile-bytes=1048576'
        ]
    elif 'road' in filename:
        # Roads - linear features
        return base_cmd + [
            '-z14',
            '-Z11',
            '--drop-rate=0.05',
            '--drop-smallest',
            '--simplification=10',
            '--buffer=16',
            '--extend-zooms-if-still-dropping',
            '--maximum-tile-bytes=1048576',
            '--coalesce-smallest-as-needed',
            '--preserve-input-order',
            '--minimum-detail=14'
        ]
    else:
        # Default for other polygon features (land, places, etc.)
        return base_cmd + [
            '-zg',
            '--simplification=10',
            '--low-detail=11',
            '--full-detail=14',
            '--drop-densest-as-needed',
            '--detect-shared-borders',
            '--maximum-tile-bytes=1048576',
            '--buffer=16',
            '--extend-zooms-if-still-dropping'
        ]

def create_tilejson():
    """Generate TileJSON for MapLibre integration - dynamically includes all available zoom-level PMTiles"""
    
    # Base TileJSON structure
    tilejson = {
        "tilejson": "3.0.0",
        "name": "Basemap prototype (Individual Zoom Levels)",
        "minzoom": 0,
        "maxzoom": 16,
        "bounds": [extent_xmin, extent_ymin, extent_xmax, extent_ymax],
        "tiles": [],
        "vector_layers": []
    }
    
    # Scan for available layer directories in the tiles directory
    layer_directories = [d for d in TILE_DIR.iterdir() if d.is_dir()]
    
    # Also scan for any remaining PMTiles in the root tiles directory (from custom processing)
    root_pmtiles = list(TILE_DIR.glob("*.pmtiles"))
    
    # Process layer directories (new structure with zoom-level PMTiles)
    for layer_dir in sorted(layer_directories):
        layer_name = layer_dir.name
        
        # Check for zoom-level PMTiles in this layer directory
        zoom_pmtiles = list(layer_dir.glob(f"{layer_name}_z*.pmtiles"))
        
        if zoom_pmtiles:
            # This is a layer directory with zoom-level PMTiles
            print(f"TILEJSON: Found layer directory: {layer_name} with {len(zoom_pmtiles)} zoom-level PMTiles")
            
            # Add each zoom-level PMTiles as a separate tile source
            for pmtiles_file in sorted(zoom_pmtiles):
                tile_url = f"pmtiles://tiles/{layer_name}/{pmtiles_file.name}"
                tilejson["tiles"].append(tile_url)
            
            # Add vector layer info for this layer
            vector_layer = {
                "id": layer_name,
                "description": f"Layer: {layer_name} (Individual zoom-level PMTiles)",
                "fields": {"id": "String", "name": "String"}  # Generic fields
            }
            tilejson["vector_layers"].append(vector_layer)
        
        # Check for building LOD PMTiles
        lod_pmtiles = list(layer_dir.glob(f"{layer_name}_*_lod_z*.pmtiles"))
        
        if lod_pmtiles:
            print(f"TILEJSON: Found building layer directory: {layer_name} with {len(lod_pmtiles)} LOD PMTiles")
            
            # Group by LOD type
            lod_groups = {}
            for pmtiles_file in lod_pmtiles:
                # Extract LOD type from filename (e.g., "buildings_low_lod_z10.pmtiles" -> "low")
                parts = pmtiles_file.stem.split('_')
                if len(parts) >= 3 and 'lod' in parts:
                    lod_idx = parts.index('lod')
                    if lod_idx > 0:
                        lod_type = parts[lod_idx - 1]
                        if lod_type not in lod_groups:
                            lod_groups[lod_type] = []
                        lod_groups[lod_type].append(pmtiles_file)
            
            # Add PMTiles for each LOD group
            for lod_type, pmtiles_files in lod_groups.items():
                for pmtiles_file in sorted(pmtiles_files):
                    tile_url = f"pmtiles://tiles/{layer_name}/{pmtiles_file.name}"
                    tilejson["tiles"].append(tile_url)
                
                # Add vector layer info for each LOD
                vector_layer = {
                    "id": f"{layer_name}_{lod_type}_lod",
                    "description": f"Layer: {layer_name} {lod_type.upper()} LOD (Individual zoom-level PMTiles)",
                    "fields": {"id": "String", "name": "String", "height": "Number"}
                }
                tilejson["vector_layers"].append(vector_layer)
    
    # Process any remaining PMTiles in root directory (from custom processing)
    for tile_file in sorted(root_pmtiles):
        tile_name = tile_file.name
        tile_url = f"pmtiles://tiles/{tile_name}"
        tilejson["tiles"].append(tile_url)
        
        # Add vector layer for custom PMTiles
        layer_name = 'layer'  # Standardized layer name for custom PMTiles
        custom_layer = {
            "id": layer_name,
            "description": f"Custom layer: {tile_file.stem}",
            "fields": {"id": "String", "name": "String"}  # Generic fields
        }
        tilejson["vector_layers"].append(custom_layer)
        print(f"TILEJSON: Added custom PMTiles layer: {layer_name} from {tile_name}")
    
    # Write TileJSON file
    tilejson_path = TILE_DIR / "tilejson.json"
    with open(tilejson_path, 'w') as f:
        json.dump(tilejson, f, indent=2)
    
    print(f"TILEJSON: TileJSON generated: {tilejson_path}")
    print(f"   - {len([t for t in tilejson['tiles'] if t.startswith('pmtiles://tiles/') and '/' in t[15:]])} layer PMTiles sources")
    print(f"   - {len([t for t in tilejson['tiles'] if t.startswith('pmtiles://tiles/') and '/' not in t[15:]])} root PMTiles sources") 
    print(f"   - {len(tilejson['vector_layers'])} vector layers")
    
    return tilejson_path

def process_individual_layer(input_file, layer_dir, layer_name):
    """Process an individual layer into individual PMTiles for each zoom level"""
    print(f"  Processing {layer_name} into individual zoom-level PMTiles...")
    
def process_individual_layer(input_file, layer_dir, layer_name):
    """Process an individual layer into individual PMTiles for each zoom level"""
    print(f"  Processing {layer_name} into individual zoom-level PMTiles...")
    
    # Generate individual PMTiles for each zoom level (6-16)
    for zoom_level in range(6, 17):
        output_pmtiles = layer_dir / f"{layer_name}_z{zoom_level}.pmtiles"
        
        # Get tippecanoe command for this zoom level
        cmd = get_individual_zoom_tippecanoe_command(input_file, output_pmtiles, layer_name, zoom_level)
        
        try:
            print(f"    Creating {layer_name}_z{zoom_level}.pmtiles...")
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            print(f"    SUCCESS: {layer_name}_z{zoom_level}.pmtiles generated")
        except subprocess.CalledProcessError as e:
            print(f"    ERROR: Failed to create {layer_name}_z{zoom_level}.pmtiles")
            print(f"    Command: {' '.join(cmd)}")
            print(f"    Error: {e.stderr}")
            continue
    
    print(f"  Completed processing {layer_name}")

def create_building_tiles_individual(input_file, layer_dir, layer_name):
    """Create building PMTiles with individual zoom levels for each LOD"""
    print(f"  Processing buildings with individual zoom PMTiles...")
    
    # Building LOD configurations
    lod_configs = {
        'low': {
            'zoom_range': {'min': 6, 'max': 11},
            'suffix': '_low_lod'
        },
        'medium': {
            'zoom_range': {'min': 11, 'max': 14},
            'suffix': '_medium_lod'
        },
        'high': {
            'zoom_range': {'min': 14, 'max': 16},
            'suffix': '_high_lod'
        }
    }
    
    for lod_type, config in lod_configs.items():
        print(f"    Processing {lod_type} LOD...")
        
        # Process each zoom level for this LOD
        for zoom in range(config['zoom_range']['min'], config['zoom_range']['max'] + 1):
            pmtiles_path = layer_dir / f"{layer_name}_{lod_type}_lod_z{zoom}.pmtiles"
            
            print(f"      Generating zoom level {zoom} for {lod_type} LOD...")
            
            # Create tippecanoe command for this specific zoom level and LOD
            cmd = get_building_zoom_tippecanoe_command(input_file, pmtiles_path, f"{layer_name}_{lod_type}_lod", lod_type, zoom)
            
            try:
                # Execute tippecanoe for this zoom level
                result = subprocess.run(cmd, check=True, capture_output=True, text=True)
                print(f"      SUCCESS: {layer_name}_{lod_type}_lod_z{zoom}.pmtiles generated")
                
            except subprocess.CalledProcessError as e:
                print(f"      ERROR: Failed to generate {lod_type} LOD zoom {zoom}: {e.stderr if e.stderr else str(e)}")
                continue
    
    print(f"  Completed processing buildings for {layer_name}")

def get_individual_zoom_tippecanoe_command(input_path, output_pmtiles, layer_name, zoom_level):
    """Get simplified tippecanoe command for generating a specific zoom level PMTiles"""
    filename = Path(input_path).name.lower()
    
    # Base command for specific zoom level
    base_cmd = [
        'tippecanoe',
        '-fo', str(output_pmtiles),
        f'-z{zoom_level}',
        f'-Z{zoom_level}',
        '-l', layer_name,
        '--clip-bounding-box', f"{extent_xmin},{extent_ymin},{extent_xmax},{extent_ymax}",
        '-P',
        str(input_path)
    ]
    
    # Add layer-specific settings based on filename patterns (simplified)
    if 'water' in filename:
        return base_cmd + [
            '--detect-shared-borders',
            '--no-tiny-polygon-reduction',
            '--buffer=64',
            '--drop-fraction-as-needed',
            '--preserve-input-order',
            '--maximum-tile-bytes=1048576'
        ]
    elif 'road' in filename:
        return base_cmd + [
            '--drop-rate=0.05',
            '--drop-smallest',
            '--simplification=10',
            '--buffer=16',
            '--coalesce-smallest-as-needed',
            '--preserve-input-order',
            '--maximum-tile-bytes=1048576'
        ]
    else:
        # Default for places, land, etc.
        return base_cmd + [
            '--simplification=10',
            '--drop-densest-as-needed',
            '--detect-shared-borders',
            '--maximum-tile-bytes=1048576',
            '--buffer=16'
        ]

def get_building_zoom_tippecanoe_command(input_path, output_pmtiles, layer_name, lod_type, zoom_level):
    """Get tippecanoe command for generating building PMTiles at a specific zoom level and LOD"""
    base_cmd = [
        'tippecanoe',
        '-fo', str(output_pmtiles),  # Use PMTiles output
        f'-z{zoom_level}',           # Maximum zoom = this specific zoom level
        f'-Z{zoom_level}',           # Minimum zoom = this specific zoom level
        '-l', layer_name,
        '--clip-bounding-box', f"{extent_xmin},{extent_ymin},{extent_xmax},{extent_ymax}",
        
        # Common building options
        '--drop-smallest',
        '--coalesce-smallest-as-needed',
        '--detect-shared-borders',
        '--preserve-input-order',
        '--maximum-tile-bytes=1048576',  # 1MB tiles
        
        '-P',
        str(input_path)
    ]
    
    # LOD-specific settings
    if lod_type == 'low':
        base_cmd.extend([
            '--simplification=10',
            '--drop-rate=0.5',
            '--buffer=8',
        ])
    elif lod_type == 'medium':
        base_cmd.extend([
            '--simplification=5',
            '--drop-rate=0.333',
            '--buffer=8',
        ])
    elif lod_type == 'high':
        base_cmd.extend([
            '--simplification=10',
            '--drop-rate=0.1',
            '--buffer=4',
        ])
    
    return base_cmd

def get_db_url(sql_section):
    """Extract URL and data type information from a SQL section"""
    # Define patterns to match different Overture data sources
    patterns = [
        # S3 patterns
        {
            'pattern': r"read_parquet\('(s3://overturemaps-us-west-2/release/[\d-]+\.\d+/theme=([^/]+)/type=([^/]+)/\*)'",
            'description_template': "Downloading {data_type} data from Overture Maps ({theme} theme)",
        },
        # Azure blob patterns
        {
            'pattern': r"read_parquet\('(az://overturemapswestus2\.blob\.core\.windows\.net/release/[\d-]+[\w.-]*/theme=([^/]+)/type=([^/]+)/\*)'",
            'description_template': "Downloading {data_type} data from Overture Maps ({theme} theme)",
        },
        # Places pattern (special case with wildcards)
        {
            'pattern': r"read_parquet\('(s3://overturemaps-us-west-2/release/[\d-]+\.\d+/theme=([^/]+)/\*)/\*'",
            'description_template': "Downloading {theme} data from Overture Maps",
        }
    ]
    
    # Extract output file path
    output_match = re.search(r"TO '([^']+)'", sql_section)
    output_file = output_match.group(1).split('/')[-1] if output_match else "unknown"
    
    # Try to match each pattern
    for pattern_info in patterns:
        match = re.search(pattern_info['pattern'], sql_section)
        if match:
            url = match.group(1)
            theme = match.group(2)
            
            # Get data type from the third group if it exists, otherwise use theme
            if len(match.groups()) >= 3:
                data_type = match.group(3)
            else:
                data_type = theme
                
            # Format the description
            description = pattern_info['description_template'].format(
                data_type=data_type.replace('_', ' ').title(),
                theme=theme.replace('_', ' ').title()
            )
            
            return {
                'url': url,
                'description': description,
                'output_file': output_file,
                'theme': theme,
                'data_type': data_type
            }
    
    return None

def get_theme_tippecanoe_command(theme_name, theme_tile_path, layer_files):
    """Get consolidated tippecanoe command for theme-based tiles"""
    base_cmd = [
        'tippecanoe',
        '-fo', str(theme_tile_path),
        '--simplification=10',  # Moderate simplification for most themes
        '--maximum-zoom=15',
        '--minimum-zoom=8',         # Points visible at lower zooms
        '--clip-bounding-box', f"{extent_xmin},{extent_ymin},{extent_xmax},{extent_ymax}",
        '--cluster-maxzoom=11',
    ]
    
    # Add layer files to command
    for layer_name, file_path in layer_files.items():
        print(f"Adding layer '{layer_name}' from file: {file_path}")
        # Use --named-layer for .geojsonseq files as recommended
        if file_path.name.endswith('.geojsonseq'):
            base_cmd.extend(['--named-layer', f'{layer_name}:{file_path}'])
        else:
            base_cmd.extend(['-L', f'{layer_name}:{file_path}'])
    
    # Add theme-specific optimizations
    if theme_name == 'settlement-extents':
        layer_settings = get_layer_tippecanoe_settings('settlement-extents', 'settlement-extents')
        base_cmd.extend(layer_settings)
    else:
        # For multi-layer themes, use settings from the primary layer type
        primary_layer_map = {
            'base': 'base-polygons',
            'transportation': 'roads',
            'places': 'places'
        }
        
        primary_layer = primary_layer_map.get(theme_name)
        if primary_layer:
            # Get the first layer file to use for settings detection
            first_layer_file = next(iter(layer_files.values())) if layer_files else None
            layer_settings = get_layer_tippecanoe_settings(primary_layer, first_layer_file)
            base_cmd.extend(layer_settings)
    
    return base_cmd

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process geospatial data into PMTiles')
    parser.add_argument('command', choices=['download', 'tiles', 'all'],
                        help='Command to execute')
    parser.add_argument('--filter', help='Only process files matching this pattern (e.g., "roads*" or "places.geojson")')
    
    args = parser.parse_args()
    
    command = args.command.lower()
    
    if command == "download":
        download_source_data()
    elif command == "tiles":
        process_to_tiles(filter_pattern=args.filter)
        create_tilejson()
    elif command == "all":
        download_source_data()
        process_to_tiles(filter_pattern=args.filter)
        create_tilejson()
    else:
        print(f"Unknown command: {command}")
        print("Usage:")
        print("  python runCreateTiles.py download                     # Download source data only")
        print("  python runCreateTiles.py tiles                        # Process to tiles only")
        print("  python runCreateTiles.py tiles --filter='roads*'      # Process only matching files")
        print("  python runCreateTiles.py all                          # Run both steps")
        sys.exit(1)