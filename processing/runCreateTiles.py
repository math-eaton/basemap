import duckdb
import os
import subprocess
import fnmatch
import re
from tqdm import tqdm
import sys
import json
import tempfile
import shutil
import argparse
import mercantile
import math
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

# Set up project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "processing" / "data"
TILE_DIR = PROJECT_ROOT / "processing" / "tiles"
OVERTURE_DATA_DIR = PROJECT_ROOT / "overture" / "data"
PUBLIC_TILES_DIR = PROJECT_ROOT / "public" / "tiles"

def deg2num(lat_deg, lon_deg, zoom):
    """Convert lat/lon to tile coordinates"""
    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return (xtile, ytile)

def tile_to_quadkey(x, y, zoom):
    """Convert tile coordinates to QuadKey"""
    quadkey = []
    for i in range(zoom, 0, -1):
        digit = 0
        mask = 1 << (i - 1)
        if (x & mask) != 0:
            digit += 1
        if (y & mask) != 0:
            digit += 2
        quadkey.append(str(digit))
    return ''.join(quadkey)

def get_quadkeys_for_extent(xmin, ymin, xmax, ymax, zoom=6):
    """Get all QuadKeys that intersect with the given extent at specified zoom level"""
    # Convert extent to tile coordinates
    x1, y1 = deg2num(ymax, xmin, zoom)  # Top-left
    x2, y2 = deg2num(ymin, xmax, zoom)  # Bottom-right
    
    # Ensure proper ordering
    x1, x2 = min(x1, x2), max(x1, x2)
    y1, y2 = min(y1, y2), max(y1, y2)
    
    quadkeys = []
    for x in range(x1, x2 + 1):
        for y in range(y1, y2 + 1):
            quadkey = tile_to_quadkey(x, y, zoom)
            quadkeys.append(quadkey)
    
    return quadkeys

def optimize_parquet_paths(base_url, quadkeys, url_type="s3"):
    """Convert base URL to QuadKey-filtered paths for faster parquet reading"""
    # Extract the base path before the wildcard
    if base_url.endswith('/*'):
        base_path = base_url[:-2]
    else:
        base_path = base_url
    
    # Generate QuadKey-specific paths
    optimized_paths = []
    for quadkey in quadkeys:
        if url_type == "s3":
            # S3 uses QuadKey partitioning like: .../quadkey=012301/part-*.parquet
            path = f"{base_path}/quadkey={quadkey}/*"
        elif url_type == "azure":
            # Azure also uses QuadKey partitioning with the same structure
            path = f"{base_path}/quadkey={quadkey}/*"
        else:
            # Default to S3 structure
            path = f"{base_path}/quadkey={quadkey}/*"
        
        optimized_paths.append(path)
    
    return optimized_paths

def optimize_sql_with_quadkeys(sql_content, quadkeys):
    """Replace S3 and Azure URLs in SQL with QuadKey-filtered paths for faster parquet reading"""
    import re
    
    # Pattern to match S3 URLs in read_parquet() calls
    s3_pattern = r"read_parquet\('(s3://overturemaps-us-west-2/release/[\d-]+\.\d+/theme=([^/]+)/type=([^/]+)/\*)'([^)]*)\)"
    
    # Pattern to match Azure URLs in read_parquet() calls
    azure_pattern = r"read_parquet\('(az://overturemapswestus2\.blob\.core\.windows\.net/release/[\d-]+[\w.-]*/theme=([^/]+)/type=([^/]+)/\*)'([^)]*)\)"
    
    # Pattern to match S3 places URLs (special case with /*/* structure)
    s3_places_pattern = r"read_parquet\('(s3://overturemaps-us-west-2/release/[\d-]+\.\d+/theme=(places)/\*)/\*'([^)]*)\)"
    
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

def process_custom_tiles(custom_inputs, filter_pattern=None):
    """Process custom/non-Overture vector data into standalone PMTiles
    
    Args:
        custom_inputs (list): List of custom input paths to process
        filter_pattern (str, optional): Only process files matching this pattern
    """
    print("=== PROCESSING CUSTOM TILES ===")
    
    # Ensure directories exist
    TILE_DIR.mkdir(parents=True, exist_ok=True)
    
    # Define search directories for custom data
    custom_data_dirs = [
        DATA_DIR,
        OVERTURE_DATA_DIR,
        PROJECT_ROOT / "processing" / "input",
        PROJECT_ROOT / "processing" / "data",
        PROJECT_ROOT / "processing" / "utilities"
    ]
    
    processed_files = []
    
    # Process each custom input
    for custom_input in custom_inputs:
        custom_path = Path(custom_input)
        
        # If not absolute path, search in data directories
        if not custom_path.is_absolute():
            found_file = None
            for data_dir in custom_data_dirs:
                potential_path = data_dir / custom_path
                if potential_path.exists():
                    found_file = potential_path
                    break
                # Also try with glob pattern
                glob_matches = list(data_dir.glob(str(custom_path)))
                if glob_matches:
                    found_file = glob_matches[0]
                    break
            
            if found_file:
                custom_path = found_file
            else:
                print(f"ERROR: Custom file not found: {custom_input}")
                print(f"   Searched in: {[str(d) for d in custom_data_dirs]}")
                continue
        
        # Check if file exists
        if not custom_path.exists():
            print(f"ERROR: Custom file not found: {custom_path}")
            continue
        
        # Apply filter if provided
        if filter_pattern and not fnmatch.fnmatch(custom_path.name, filter_pattern):
            print(f"SKIPPING {custom_path.name} (doesn't match filter: {filter_pattern})")
            continue
        
        # Process the file
        print(f"\n--- Processing custom file: {custom_path.name} ---")
        
        # Determine output name (remove .geojsonseq/.geojson extension)
        base_name = custom_path.stem
        if base_name.endswith('.geojsonseq'):
            base_name = base_name[:-12]  # Remove .geojsonseq
        
        tile_path = TILE_DIR / f"{base_name}.pmtiles"
        
        # Get optimized tippecanoe command
        # Use consistent "layer" name for all PMTiles to ensure interoperability
        layer_name = 'layer'  # Standardized layer name for all PMTiles
        cmd = get_tippecanoe_command(custom_path, tile_path, layer_name)
        
        # Execute tippecanoe
        try:
            print(f"Generating {base_name}.pmtiles...")
            result = subprocess.run(cmd, check=True, text=True)
            print(f"SUCCESS: {base_name}.pmtiles generated successfully")
            processed_files.append({
                'name': base_name,
                'path': tile_path,
                'layer': layer_name,
                'source': custom_path
            })
        except subprocess.CalledProcessError as e:
            print(f"ERROR: Error generating {base_name}.pmtiles:")
            print(f"   Command: {' '.join(cmd)}")
            print(f"   Error: {e.stderr if e.stderr else str(e)}")
        except Exception as e:
            print(f"ERROR: {str(e)}")
    
    if processed_files:
        print(f"\nSUCCESS: Successfully processed {len(processed_files)} custom files:")
        for file_info in processed_files:
            print(f"   - {file_info['name']}.pmtiles (layer: {file_info['layer']})")
    else:
        print("\nWARNING: No custom files were processed")
    
    print("=== CUSTOM TILES PROCESSING COMPLETE ===\n")
    return processed_files

def process_to_tiles(custom_inputs=None, filter_pattern=None, theme_filter=None):
    """Process GeoJSON/GeoJSONSeq files into theme-based PMTiles
    
    Args:
        custom_inputs (list, optional): List of custom input paths to process first
        filter_pattern (str, optional): Only process files matching this pattern (e.g., 'roads*')
        theme_filter (str, optional): Only process specific theme (e.g., 'base', 'transportation')
    """
    print("=== PROCESSING TO TILES ===")
    
    # Ensure directories exist
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OVERTURE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    TILE_DIR.mkdir(parents=True, exist_ok=True)
    
    # First, process any custom inputs as standalone tiles
    if custom_inputs:
        process_custom_tiles(custom_inputs, filter_pattern)
    
    # Define themes and their corresponding files
    themes = {
        'base': {
            'files': ['land_use.geojsonseq', 'land_cover.geojsonseq', 'land_residential.geojsonseq', 'water.geojsonseq', 'infrastructure.geojsonseq'],
            'layers': {
                # 'land': 'land.geojsonseq',
                'land_use': 'land_use.geojsonseq',
                'land_cover': 'land_cover.geojsonseq',
                'land_residential': 'land_residential.geojsonseq',
                'water': 'water.geojsonseq',
                'infrastructure': 'infrastructure.geojsonseq'
            }
        },
        'settlement-extents': {
            'files': ['*extents*.geojsonseq'],
            'layers': {
                'settlementextents': '*extent*.geojsonseq'
            }
        },
        'transportation': {
            'files': ['roads.geojsonseq'],
            'layers': {
                'roads': 'roads.geojsonseq'
            }
        },
        'places': {
            'files': ['places.geojson', 'placenames.geojson', 'GRID3_COD_health_facilities_v5_0.geojson', 'GRID3_COD_settlement_names_v5_0.geojson'],
            'layers': {
                'places': 'places.geojson',
                'placenames': 'placenames.geojson',
                'health_facilities': 'GRID3_COD_health_facilities_v5_0.geojson',
                'settlement_names': 'GRID3_COD_settlement_names_v5_0.geojson'
            }
        },
        'admin': {
            'files': ['GRID3_COD_health_areas_v5_0.geojson', 'GRID3_COD_health_zones_v5_0.geojson'],
            'layers': {
                'health_areas': 'GRID3_COD_health_areas_v5_0.geojson',
                'health_zones': 'GRID3_COD_health_zones_v5_0.geojson'
            }
        },
        'buildings': {
            'files': ['buildings.geojsonseq'],
            'layers': {
                'buildings': 'buildings.geojsonseq'
            },
            'multi_lod': True  # Special handling for buildings
        }
    }
    
    # Process each theme
    for theme_name, theme_config in themes.items():
        # Apply theme filter if provided
        if theme_filter and theme_name != theme_filter:
            print(f"Skipping {theme_name} theme (doesn't match filter: {theme_filter})")
            continue
            
        print(f"\n--- Processing {theme_name} theme ---")
        
        # Find files for this theme
        theme_files = []
        for file_pattern in theme_config['files']:
            # Check in both data directories
            for data_dir in [DATA_DIR, OVERTURE_DATA_DIR]:
                matching_files = list(data_dir.glob(file_pattern))
                theme_files.extend(matching_files)
        
        # Apply filter if provided
        if filter_pattern:
            theme_files = [f for f in theme_files if fnmatch.fnmatch(f.name, filter_pattern)]
        
        if not theme_files:
            print(f"No files found for {theme_name} theme")
            continue
            
        # Special handling for buildings (multi-LOD)
        if theme_config.get('multi_lod') and any('building' in f.name for f in theme_files):
            print(f"Processing buildings with multi-LOD approach...")
            building_files = [f for f in theme_files if 'building' in f.name]
            for building_file in building_files:
                create_building_tiles(building_file, TILE_DIR, building_file.name)
            continue
        
        # Build tippecanoe command for this theme
        theme_tile_path = TILE_DIR / f"{theme_name}.pmtiles"
        
        # Collect layer files for this theme
        layer_files = {}
        for layer_name, file_pattern in theme_config['layers'].items():
            # Use fnmatch for pattern matching instead of exact match
            matching_files = [f for f in theme_files if fnmatch.fnmatch(f.name, file_pattern)]
            if matching_files:
                layer_files[layer_name] = matching_files[0]
        
        # Get consolidated tippecanoe command
        cmd = get_theme_tippecanoe_command(theme_name, theme_tile_path, layer_files)
        
        # Execute tippecanoe
        try:
            print(f"Generating {theme_name}.pmtiles...")
            print(f"Command: {' '.join(cmd)}")
            
            # Run tippecanoe command
            result = subprocess.run(cmd, check=True, text=True)
            print(f"SUCCESS: {theme_name}.pmtiles generated successfully")
                
        except subprocess.CalledProcessError as e:
            print(f"ERROR: Error generating {theme_name}.pmtiles: {e.stderr.decode() if e.stderr else str(e)}")
        except Exception as e:
            print(f"ERROR: {str(e)}")

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
    
    This function now returns only truly layer-specific options.
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
            '--simplification=2',        # Reduced for better coastline detail (better than default)
            '--low-detail=11',           # Earlier detail start
            '--full-detail=13',          # Full detail at zoom 13 to match base
            '--no-tiny-polygon-reduction',
            '--no-feature-limit',
            '--extend-zooms-if-still-dropping',
            '--maximum-tile-bytes=2097152',  # 2MB for water features (override base)
            '--maximum-zoom=15',         # Extended to match base polygons
            '--gamma=0.9',               # Less aggressive for water bodies
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
            '--cluster-distance=35',     # Unique to point features
            '--drop-rate=0.1',
        ]
    
    elif layer_type == 'base-polygons':
        # Optimized for base polygon layers (land_use, land_cover, etc.)
        settings = [
            '--simplification=5',        # More aggressive simplification reduction
            '--drop-rate=0.1',          # Very conservative dropping to keep features
            '--low-detail=10',          # Start detail reduction later
            '--full-detail=14',         # Higher quality full detail
            '--coalesce-smallest-as-needed',
            '--maximum-zoom=15',        # Extended for high detail
            '--minimum-zoom=9',
            # '--cluster-distance=25',    # Tighter clustering for more detail
            '--no-tiny-polygon-reduction',  # Preserve small polygons at high zoom
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
                '--no-tiny-polygon-reduction',  # Preserve small polygons
            ]
        
        else:
            # Mixed or Unknown geometry types - use conservative polygon defaults
            settings = [
                '--simplification=3',        # Conservative simplification
                '--drop-rate=0.08',         # Very conservative dropping
                '--low-detail=9',           # Early detail preservation
                '--full-detail=12',         
                '--coalesce-smallest-as-needed',
                '--extend-zooms-if-still-dropping',
                '--gamma=0.4',              # Moderate density reduction
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

def get_tippecanoe_command(input_path, tile_path, layer_name):
    """Get optimized tippecanoe command based on layer name and file type"""
    # Base command with common high-quality settings moved from layer-specific options
    base_cmd = [
        'tippecanoe',
        '-fo', str(tile_path),
        '-zg',
        '-l', layer_name,
        '--single-precision',
        # Clip to the same extent as other tiles
        '--clip-bounding-box', f"{extent_xmin},{extent_ymin},{extent_xmax},{extent_ymax}",
        
        # Common high-quality options consolidated from layer types
        '--buffer=8',                    # Most layers use 8, higher quality than 4
        '--no-polygon-splitting',        # Used by most polygon layers
        '--detect-shared-borders',       # Used by most polygon layers
        '--drop-smallest',               # Used by most layers for quality
        '--maximum-tile-bytes=1048576',  # Standard 1MB tiles across most layers
        '--preserve-input-order',        # Used by all layers for consistency
        '--coalesce-densest-as-needed',  # Used by most layers
        '--drop-fraction-as-needed',     # Used by most layers
        
        '-P',
        str(input_path)
    ]
    
    # Get layer-specific settings (now only the truly unique options)
    layer_settings = get_layer_tippecanoe_settings(layer_name, input_path)
    
    return base_cmd + layer_settings

def get_building_tippecanoe_command(input_path, tile_path, layer_name, lod_type, zoom_range):
    """Get consolidated tippecanoe command for building tiles with LOD-specific settings"""
    base_cmd = [
        'tippecanoe',
        '-fo', str(tile_path),
        f'-z{zoom_range["max"]}',
        f'-Z{zoom_range["min"]}',
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

def create_tilejson():
    """Generate TileJSON for MapLibre integration - dynamically includes all available PMTiles"""
    
    # Base TileJSON structure
    tilejson = {
        "tilejson": "3.0.0",
        "name": "Basemap prototype",
        "minzoom": 0,
        "maxzoom": 14,
        "bounds": [extent_xmin, extent_ymin, extent_xmax, extent_ymax],
        "tiles": [],
        "vector_layers": []
    }
    
    # Scan for available PMTiles in the tiles directory
    available_tiles = list(TILE_DIR.glob("*.pmtiles"))
    
    # Add core theme tiles (these have known structures)
    core_tiles = {
        "base.pmtiles": {
            "layers": [
                # {"id": "land", "description": "Land polygons", "fields": {"subtype": "String", "class": "String"}},
                {"id": "land_use", "description": "Land use polygons", "fields": {"subtype": "String", "class": "String"}},
                {"id": "land_residential", "description": "Residential areas", "fields": {"subtype": "String", "class": "String"}},
                {"id": "water", "description": "Water bodies", "fields": {"subtype": "String", "class": "String"}},
                {"id": "infrastructure", "description": "Infrastructure", "fields": {"subtype": "String", "class": "String"}},
            ]
        },
        "settlement-extents.pmtiles": {
            "layers": [
                {"id": "settlementextents", "description": "Settlement boundary extents", "fields": {"name": "String", "type": "String", "id": "String"}},
            ]
        },
        "transportation.pmtiles": {
            "layers": [
                {"id": "roads", "description": "Road network", "fields": {"class": "String", "subclass": "String"}},
            ]
        },
        "places.pmtiles": {
            "layers": [
                {"id": "places", "description": "Points of interest", "fields": {"category": "String", "confidence": "Number"}},
                {"id": "placenames", "description": "Place names", "fields": {"subtype": "String", "locality_type": "String"}},
                {"id": "health_facilities", "description": "Health facilities", "fields": {"name": "String", "type": "String", "id": "String"}},
                {"id": "settlement_names", "description": "Settlement names", "fields": {"name": "String", "type": "String", "id": "String"}},
            ]
        },
        "admin.pmtiles": {
            "layers": [
                {"id": "health_areas", "description": "Health administrative areas", "fields": {"name": "String", "type": "String", "id": "String"}},
                {"id": "health_zones", "description": "Health administrative zones", "fields": {"name": "String", "type": "String", "id": "String"}},
            ]
        },
        "buildings_low_lod.pmtiles": {
            "layers": [
                {"id": "buildings", "description": "Buildings (Low LOD)", "fields": {"name": "String", "height": "Number", "level": "Number"}},
            ]
        },
        "buildings_medium_lod.pmtiles": {
            "layers": [
                {"id": "buildings", "description": "Buildings (Medium LOD)", "fields": {"name": "String", "height": "Number", "level": "Number"}},
            ]
        },
        "buildings_high_lod.pmtiles": {
            "layers": [
                {"id": "buildings", "description": "Buildings (High LOD)", "fields": {"name": "String", "height": "Number", "level": "Number"}},
            ]
        }
    }
    
    # Add tiles and vector layers for available PMTiles
    for tile_file in sorted(available_tiles):
        tile_name = tile_file.name
        tile_url = f"pmtiles://tiles/{tile_name}"
        tilejson["tiles"].append(tile_url)
        
        # Add vector layers if this is a known core tile
        if tile_name in core_tiles:
            tilejson["vector_layers"].extend(core_tiles[tile_name]["layers"])
        else:
            # For custom tiles, use consistent "layer" name for interoperability
            layer_name = 'layer'  # Standardized layer name for all custom PMTiles
            custom_layer = {
                "id": layer_name,
                "description": f"Custom layer: {tile_file.stem}",
                "fields": {"id": "String", "name": "String"}  # Generic fields
            }
            tilejson["vector_layers"].append(custom_layer)
            print(f"TILEJSON: Added custom layer to TileJSON: {layer_name} from {tile_name}")
    
    # Write TileJSON file
    tilejson_path = TILE_DIR / "tilejson.json"
    with open(tilejson_path, 'w') as f:
        json.dump(tilejson, f, indent=2)
    
    print(f"TILEJSON: TileJSON generated: {tilejson_path}")
    print(f"   - {len(tilejson['tiles'])} PMTiles sources")
    print(f"   - {len(tilejson['vector_layers'])} vector layers")
    
    return tilejson_path

def create_building_tiles(input_path, tile_dir, geojson_file, skip_low_lod=False, skip_medium_lod=False, skip_high_lod=True):
    """Create separate low-LOD, medium-LOD, and high-LOD building tiles for smooth crossfading"""
    layer_name = 'layer'
    base_name = Path(geojson_file).stem

    # Building LOD configurations
    lod_configs = {
        'low': {
            'skip': skip_low_lod,
            'zoom_range': {'min': 0, 'max': 9},
            'suffix': '_low_lod'
        },
        'medium': {
            'skip': skip_medium_lod,
            'zoom_range': {'min': 11, 'max': 13},
            'suffix': '_medium_lod'
        },
        'high': {
            'skip': skip_high_lod,
            'zoom_range': {'min': 13, 'max': 15},
            'suffix': '_high_lod'
        }
    }

    for lod_type, config in lod_configs.items():
        if config['skip']:
            continue
            
        lod_path = tile_dir / f"{base_name}{config['suffix']}.pmtiles"
        print(f"Generating {lod_type}-LOD building tiles for {geojson_file}...")

        try:
            cmd = get_building_tippecanoe_command(input_path, lod_path, layer_name, lod_type, config['zoom_range'])
            subprocess.run(cmd, check=True)
            print(f"{lod_type.title()}-LOD building tiles generated successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error generating {lod_type}-LOD building tiles: {e}")
            return

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
        '-zg',
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
            layer_settings = get_layer_tippecanoe_settings(primary_layer)
            base_cmd.extend(layer_settings)
    
    return base_cmd

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process geospatial data into PMTiles')
    parser.add_argument('command', choices=['download', 'tiles', 'all', 'convert', 'custom'],
                        help='Command to execute')
    parser.add_argument('--input', nargs='+', help='Custom input file(s) to process')
    parser.add_argument('--filter', help='Only process files matching this pattern')
    parser.add_argument('--theme', help='Only process specific theme (base, transportation, places, buildings, settlement-extents)')
    parser.add_argument('--layer', help='Layer name for custom input (for multi-layer sources)')
    parser.add_argument('--output', help='Output file path (for convert command)')
    parser.add_argument('--where', help='SQL WHERE clause to filter features (for convert command)')
    parser.add_argument('--reproject', help='Reproject to CRS (e.g., EPSG:4326) (for convert command)')
    
    args = parser.parse_args()
    
    command = args.command.lower()
    
    if command == "download":
        download_source_data()
    elif command == "tiles":
        process_to_tiles(custom_inputs=args.input, filter_pattern=args.filter, theme_filter=args.theme)
        create_tilejson()
    elif command == "all":
        download_source_data()
        process_to_tiles(custom_inputs=args.input, filter_pattern=args.filter, theme_filter=args.theme)
        create_tilejson()
    elif command == "custom":
        if not args.input:
            print("Error: --input is required for 'custom' command")
            sys.exit(1)
        process_custom_tiles(args.input, filter_pattern=args.filter)
        create_tilejson()
    elif command == "convert":
        if not args.input:
            print("Error: --input is required for 'convert' command")
            sys.exit(1)
        
        # Import the converter utility
        try:
            sys.path.append(str(PROJECT_ROOT / "processing" / "utilities"))
            from convertForTipp import convert_file
        except ImportError as e:
            print(f"Error importing convertForTipp: {e}")
            print("Make sure convertForTipp.py is in the utilities directory")
            sys.exit(1)
        
        # Process each input file
        for input_file in args.input:
            input_path = Path(input_file)
            
            # Determine output path
            if args.output:
                output_path = Path(args.output)
            else:
                # Default: place in processing/data with .geojsonseq extension
                output_path = DATA_DIR / f"{input_path.stem}.geojsonseq"
            
            print(f"Converting {input_path} to {output_path}")
            
            # Convert the file
            try:
                convert_kwargs = {
                    'layer_name': args.layer,
                    'where': args.where,
                    'reproject': args.reproject,
                    'verbose': True
                }
                
                processed, skipped, output = convert_file(
                    str(input_path), 
                    str(output_path), 
                    **convert_kwargs
                )
                
                print(f"SUCCESS: Conversion completed: {processed} features processed, {skipped} skipped")
                print(f"OUTPUT: {output}")
                
            except Exception as e:
                print(f"ERROR: Error converting {input_file}: {e}")
    else:
        print(f"Unknown command: {command}")
        
        print("Usage:")
        print("  python runCreateTiles.py download                                    # Download source data only")
        print("  python runCreateTiles.py tiles                                       # Process to tiles only")
        print("  python runCreateTiles.py tiles --theme=base                         # Process only base theme")
        print("  python runCreateTiles.py tiles --input settlement.geojsonseq        # Process with custom files")
        print("  python runCreateTiles.py tiles --filter='roads*'                    # Process only roads files")
        print("  python runCreateTiles.py custom --input settlement.geojsonseq       # Process only custom files")
        print("  python runCreateTiles.py all                                        # Run both steps")
        print("  python runCreateTiles.py convert --input file.gpkg --layer=layer1   # Convert custom file to GeoJSONSeq")
        print("  python runCreateTiles.py convert --input file.shp --reproject=EPSG:4326  # Convert and reproject")
        sys.exit(1)