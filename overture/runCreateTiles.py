import duckdb
import os
import subprocess
import fnmatch

# extent parameters for New York State
# extent_xmin = -79.76259
# extent_xmax = -71.85621
# extent_ymin = 40.49612
# extent_ymax = 45.01585

# extent parameters for st lawrence county, ny
extent_xmin = -75.5
extent_xmax = -74.5
extent_ymin = 44.0
extent_ymax = 45.0


def download_source_data():
    """Download and process source data from Overture Maps"""
    print("=== DOWNLOADING SOURCE DATA ===")
        
    # Path to your SQL file
    sql_file_path = '/Users/matthewheaton/GitHub/basemap/overture/createLandTiles'

    # Read the SQL file
    with open(sql_file_path, 'r') as file:
        sql_content = file.read()

    # Replace the variables in the SQL content
    sql_content = sql_content.replace('$extent_xmin', str(extent_xmin))
    sql_content = sql_content.replace('$extent_xmax', str(extent_xmax))
    sql_content = sql_content.replace('$extent_ymin', str(extent_ymin))
    sql_content = sql_content.replace('$extent_ymax', str(extent_ymax))

    # Split the SQL content into sections based on '-- breakpoint'
    sql_sections = sql_content.split('-- breakpoint')

    # Connect to DuckDB
    conn = duckdb.connect()

    # Execute each section
    for i, section in enumerate(sql_sections):
        section = section.strip()
        if section and not section.startswith('SET extent_'):  # Skip empty sections and SET commands
            print(f"Executing section {i + 1}...")
            try:
                conn.execute(section)
                print(f"Section {i + 1} executed successfully.")
            except Exception as e:
                print(f"Error executing section {i + 1}: {e}")
                print(f"Section content: {section[:200]}...")

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
        # optionally filter for specific features e.g. buildings
        and 'buildings' in f 
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
                # preserve polygon topology
                subprocess.run([
                    'tippecanoe',
                    '-fo', tile_path,
                    '-zg',
                    '-l', layer_name,
                    '--detect-shared-borders',  # Better polygon boundary handling
                    # '--simplify-only-low-zooms',  # Preserve detail at high zoom
                    '--no-tiny-polygon-reduction',  # Keep small bodies
                    '--no-feature-limit',  # Don't limit features per tile
                    '--no-tile-size-limit',  # Don't limit tile size
                    '--buffer=64',  # Add buffer to prevent edge artifacts
                    '--drop-fraction-as-needed',  # Better than dropping whole features
                    '--preserve-input-order',  # Maintain feature order from input
                    '--coalesce-densest-as-needed',  # Better polygon merging
                    '--extend-zooms-if-still-dropping',  # Keep trying to fit all features
                    '-P',
                    input_path
                ], check=True)
            elif 'roads' in geojson_file:
                # Optimized settings for road features - better visibility across zoom levels
                subprocess.run([
                    'tippecanoe',
                    '-fo', tile_path,
                    '-z16',  # Match max zoom of map
                    '-Z11',  # Start at map's minimum zoom level
                    '-l', layer_name,
                    '--simplify-only-low-zooms',  # Keep detail at high zoom levels
                    '--drop-rate=0.01',  # Keep most features, drop only 1%
                    '--drop-smallest',  # Drop smallest features first
                    '--simplification=4',  # Use moderate simplification
                    '--buffer=64',  # Larger buffer for smoother line rendering
                    '--extend-zooms-if-still-dropping',
                    '--maximum-tile-bytes=2097152',  # 2MB tile limit for roads
                    '--coalesce-smallest-as-needed',  # Merge small road segments
                    '--preserve-input-order',  # Maintain feature order
                    '--minimum-detail=12',  # Start preserving full detail at zoom 12
                    '-P',
                    input_path
                ], check=True)
            elif 'building' in geojson_file:
                # Optimized settings for building features - balanced detail and performance
                subprocess.run([
                    'tippecanoe',
                    '-fo', tile_path,
                    '-zg', # best fit
                    # '-z16',  # Match max zoom of map
                    # '-Z13',  # Start showing buildings at zoom 13
                    '-l', layer_name,
                    # '--simplify-only-low-zooms',  # Keep building detail at high zoom
                    '--drop-rate=0.03',  # Drop 3% of features if needed
                    '--drop-smallest',  # Prioritize larger buildings
                    '--no-tiny-polygon-reduction',  # Keep small building shapes
                    '--buffer=16',  # Medium buffer for clean edges
                    '--extend-zooms-if-still-dropping',
                    '--maximum-tile-bytes=1048576',  # 1MB tile limit for performance
                    '--coalesce-smallest-as-needed',  # Merge very small buildings
                    # '--minimum-detail=14',  # Preserve full detail at zoom 14+
                    '--detect-shared-borders',  # Better handling of adjacent buildings
                    '-P',
                    input_path
                ], check=True)
            else:
                # Default settings for other features
                subprocess.run([
                    'tippecanoe',
                    '-fo', tile_path,
                    '-zg',
                    '-l', layer_name,
                    '--drop-densest-as-needed',
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