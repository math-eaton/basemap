import duckdb
import os
import subprocess
import fnmatch
import re

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
extent_xmin = 22.0
extent_xmax = 24.0
extent_ymin = -6.0
extent_ymax = -4.0


# Buffer for data download to ensure complete features at edges
buffer_degrees = 1  # ~111km buffer

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
        
    # Path to SQL file
    sql_file_path = '/Users/matthewheaton/GitHub/basemap/overture/tileQueries'

    # Read the SQL file
    with open(sql_file_path, 'r') as file:
        sql_content = file.read()

    # Replace the variables in the SQL content with buffered extent
    sql_content = sql_content.replace('$extent_xmin', str(buffered_xmin))
    sql_content = sql_content.replace('$extent_xmax', str(buffered_xmax))
    sql_content = sql_content.replace('$extent_ymin', str(buffered_ymin))
    sql_content = sql_content.replace('$extent_ymax', str(buffered_ymax))

    # Split the SQL content into sections based on '-- breakpoint'
    sql_sections = sql_content.split('-- breakpoint')

    # Connect to DuckDB
    conn = duckdb.connect()

    # Execute each section
    for i, section in enumerate(sql_sections):
        section = section.strip()
        if section and not section.startswith('SET extent_'):  # Skip empty sections and SET commands
            # Extract URL and data type from the section
            url_info = get_db_url(section)
            if url_info:
                print(f"Executing section {i + 1}: {url_info['description']}")
                print(f"  → Querying: {url_info['url']}")
                print(f"  → Output: {url_info['output_file']}")
            else:
                print(f"Executing section {i + 1}...")
            
            try:
                conn.execute(section)
                print(f"  ✓ Section {i + 1} executed successfully.")
            except Exception as e:
                print(f"  ✗ Error executing section {i + 1}: {e}")
                print(f"  Section content: {section[:200]}...")

    # Close the connection
    conn.close()
    print("=== SOURCE DATA DOWNLOAD COMPLETE ===\n")

def process_to_tiles():
    """Process GeoJSON/GeoJSONSeq files into PMTiles"""
    print("=== PROCESSING TO TILES ===")
    
    # Path to the directory containing the GeoJSON/GeoJSONSeq files
    data_dir = '/Users/matthewheaton/GitHub/basemap/overture/data/'
    tile_dir = '/Users/matthewheaton/GitHub/basemap/overture/tiles/'
    
    # Ensure directories exist
    os.makedirs(tile_dir, exist_ok=True)

    # Find all files in data_dir that are GeoJSON/GeoJSONSeq and not .pmtiles
    geojson_files = [
        f for f in os.listdir(data_dir)
        if (f.endswith('.geojson') or f.endswith('.geojsonseq')) and not f.endswith('.pmtiles') 
        # Process all files for now - we'll handle buildings specially
        and 'building' in f 
        # and 'buildings' not in f  # Uncomment to exclude buildings
    ]

    if not geojson_files:
        print("No GeoJSON/GeoJSONSeq files found. Run download_source_data() first.")
        return

    print(f"Found {len(geojson_files)} files to process:")
    for f in geojson_files:
        print(f"  - {f}")

    # Tippecanoe CLI options
    layer_name = 'layer'

    # Process each GeoJSON/GeoJSONSeq file into tiles
    for geojson_file in geojson_files:
        input_path = os.path.join(data_dir, geojson_file)
        tile_path = os.path.join(tile_dir, f"{os.path.splitext(geojson_file)[0]}.pmtiles")
        
        # Check if input file exists
        if not os.path.exists(input_path):
            print(f"Warning: {input_path} does not exist, skipping...")
            continue
            
        print(f"Generating tiles for {geojson_file}...")
        try:
            # tippecanoe settings based on file type
            if 'water' in geojson_file:
                # Preserve polygon topology with optimized simplification
                subprocess.run([
                    'tippecanoe',
                    '-fo', tile_path,
                    '-zg',
                    '-l', layer_name,
                    '--detect-shared-borders',  # Better polygon boundary handling
                    # '--simplification=10',  # Less aggressive simplification for higher quality
                    '--no-tiny-polygon-reduction',  # Preserve small water bodies
                    '--low-detail=13',  # Simplified geometry until zoom 13
                    '--full-detail=15',  # Full detail starting at zoom 15
                    '--no-feature-limit',  # Don't limit features per tile
                    '--buffer=64',  # Moderate buffer to prevent edge artifacts
                    '--drop-fraction-as-needed',  # Better than dropping whole features
                    '--preserve-input-order',  # Maintain feature order from input
                    '--coalesce-densest-as-needed',  # Better polygon merging
                    '--extend-zooms-if-still-dropping',  # Keep trying to fit all features
                    '--maximum-tile-bytes=1048576',  # 1MB tile limit for higher quality
                    '-P',
                    input_path
                ], check=True)
            elif 'roads' in geojson_file:
                subprocess.run([
                    'tippecanoe',
                    '-fo', tile_path,
                    '-z14',  # Match max zoom of map
                    '-Z11',  # Start at map's minimum zoom level
                    '-l', layer_name,
                    # '--simplify-only-low-zooms',  # Keep detail at high zoom levels
                    '--drop-rate=0.05',  # Keep most features, drop only N%
                    '--drop-smallest',  # Drop smallest features first
                    '--simplification=10',  # Use moderate simplification
                    '--buffer=16',  # buffer for smoother line rendering
                    '--extend-zooms-if-still-dropping',
                    '--maximum-tile-bytes=1048576',  # 1MB tile limit for roads
                    '--coalesce-smallest-as-needed',  # Merge small road segments
                    '--preserve-input-order',  # Maintain feature order
                    '--minimum-detail=14',  # Start preserving full detail at zoom 15
                    '-P',
                    input_path
                ], check=True)
            elif 'building' in geojson_file:
                # Special handling for buildings - create both low-LOD and high-LOD versions
                create_building_tiles(input_path, tile_dir, geojson_file)
            else:
                # Default settings for other polygon features (land, land_use, etc.)
                subprocess.run([
                    'tippecanoe',
                    '-fo', tile_path,
                    '-zg',
                    '-l', layer_name,
                    '--simplification=10',
                    '--low-detail=11',  # Simplified geometry until zoom 11
                    '--full-detail=14',  # Full detail starting at zoom 14
                    '--drop-densest-as-needed',
                    '--detect-shared-borders',  # Better polygon boundary handling
                    '--maximum-tile-bytes=1048576',  # 1MB tile limit for smaller sizes
                    '--buffer=16',  # Moderate buffer
                    '--extend-zooms-if-still-dropping',
                    '-P',
                    input_path
                ], check=True)
            print(f"Tiles for {geojson_file} generated successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error generating tiles for {geojson_file}: {e}")
        except FileNotFoundError:
            print("Error: tippecanoe not found. Please make sure it's installed and in your PATH.")
            break

    print("=== TILE PROCESSING COMPLETE ===\n")

def create_building_tiles(input_path, tile_dir, geojson_file, skip_low_lod=False, skip_medium_lod=False, skip_high_lod=False):
    """Create separate low-LOD, medium-LOD, and high-LOD building tiles for smooth crossfading"""
    layer_name = 'layer'
    base_name = os.path.splitext(geojson_file)[0]

    if not skip_low_lod:
        # Low-LOD buildings: aggressive simplification for performance
        low_lod_path = os.path.join(tile_dir, f"{base_name}_low_lod.pmtiles")
        print(f"Generating low-LOD building tiles for {geojson_file}...")

        try:
            subprocess.run([
                'tippecanoe',
                '-fo', low_lod_path,
                '-z9',  # Lower max zoom for low-LOD
                '-Z0',  # Start at zoom 0
                '-l', layer_name,
                '--simplification=15',  # Higher simplification for smaller tile size
                '--drop-rate=0.7',     # Drop 70% of features aggressively
                '--drop-smallest',     # Drop smallest buildings first
                '--buffer=2',          # Smaller buffer for tighter tiles
                '--maximum-tile-bytes=65536',  # 64KB tiles for better performance
                '--coalesce-smallest-as-needed',
                '--detect-shared-borders',
                '-P',
                input_path
            ], check=True)
            print(f"  ✓ Low-LOD building tiles generated successfully.")
        except subprocess.CalledProcessError as e:
            print(f"  ✗ Error generating low-LOD building tiles: {e}")
            return

    if not skip_medium_lod:
        # Medium-LOD buildings: balance detail and performance
        medium_lod_path = os.path.join(tile_dir, f"{base_name}_medium_lod.pmtiles")
        print(f"Generating medium-LOD building tiles for {geojson_file}...")

        try:
            subprocess.run([
                'tippecanoe',
                '-fo', medium_lod_path,
                '-z13',  # Max zoom for medium-LOD
                '-Z10',  # Start at zoom 10
                '-l', layer_name,
                '--simplification=10',  # Moderate simplification
                '--drop-rate=0.4',     # Drop 40% of features
                '--drop-smallest',     # Drop smallest buildings first
                '--buffer=4',          # Moderate buffer
                '--maximum-tile-bytes=131072',  # 128KB tiles for better performance
                '--coalesce-smallest-as-needed',
                '--detect-shared-borders',
                '-P',
                input_path
            ], check=True)
            print(f"  ✓ Medium-LOD building tiles generated successfully.")
        except subprocess.CalledProcessError as e:
            print(f"  ✗ Error generating medium-LOD building tiles: {e}")
            return

    if not skip_high_lod:
        # High-LOD buildings: preserve more detail, start later for clear distinction
        high_lod_path = os.path.join(tile_dir, f"{base_name}_high_lod.pmtiles")
        print(f"Generating high-LOD building tiles for {geojson_file}...")

        try:
            subprocess.run([
                'tippecanoe',
                '-fo', high_lod_path,
                '-z16',  # Higher max zoom for high-LOD
                '-Z14',  # Start at zoom 14
                '-l', layer_name,
                '--simplification=8',  # Lower simplification for higher detail
                '--drop-rate=0.2',     # Drop 20% of features
                '--drop-smallest',     # Drop smallest buildings first
                '--buffer=8',          # Larger buffer for smoother transitions
                '--maximum-tile-bytes=524288',  # 512KB tiles for better detail
                '--coalesce-smallest-as-needed',
                '--detect-shared-borders',
                '--preserve-input-order',
                '-P',
                input_path
            ], check=True)
            print(f"  ✓ High-LOD building tiles generated successfully.")
        except subprocess.CalledProcessError as e:
            print(f"  ✗ Error generating high-LOD building tiles: {e}")

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

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python runCreateTiles.py download    # Download source data only")
        print("  python runCreateTiles.py tiles       # Process to tiles only")
        print("  python runCreateTiles.py all         # Run both steps")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "download":
        download_source_data()
    elif command == "tiles":
        process_to_tiles()
    elif command == "all":
        download_source_data()
        process_to_tiles()
    else:
        print(f"Unknown command: {command}")
        print("Use: download, tiles, or all")