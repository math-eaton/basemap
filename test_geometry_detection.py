#!/usr/bin/env python3

import json
from pathlib import Path

def detect_geometry_type(file_path):
    """Detect the primary geometry type from a GeoJSON or GeoJSONSeq file"""
    print(f"Checking file: {file_path}")
    print(f"File exists: {file_path.exists()}")
    print(f"File suffix: {file_path.suffix}")
    
    try:
        geometry_types = set()
        sample_count = 0
        max_samples = 10  # Just sample a few for testing
        
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
                                print("  Detected line-delimited JSON format!")
                        except json.JSONDecodeError:
                            pass
                f.seek(0)  # Reset file pointer again
            except json.JSONDecodeError:
                pass
            
            if file_path.suffix == '.geojsonseq' or is_line_delimited:
                print("Processing as GeoJSONSeq/line-delimited...")
                for line_num, line in enumerate(f):
                    line = line.strip()
                    if line and sample_count < max_samples:
                        try:
                            feature = json.loads(line)
                            if 'geometry' in feature and feature['geometry'] and 'type' in feature['geometry']:
                                geom_type = feature['geometry']['type']
                                geometry_types.add(geom_type)
                                sample_count += 1
                                print(f"    Line {line_num}: {geom_type}")
                        except json.JSONDecodeError as e:
                            print(f"    JSON decode error on line {line_num}: {e}")
                            continue
            else:
                print("Processing as regular GeoJSON...")
                try:
                    data = json.load(f)
                    if 'features' in data:
                        print(f"  Found {len(data['features'])} features")
                        for i, feature in enumerate(data['features'][:max_samples]):
                            if 'geometry' in feature and feature['geometry'] and 'type' in feature['geometry']:
                                geom_type = feature['geometry']['type']
                                geometry_types.add(geom_type)
                                sample_count += 1
                                print(f"    Feature {i}: {geom_type}")
                    elif 'geometry' in data and data['geometry'] and 'type' in data['geometry']:
                        # Single feature GeoJSON
                        geometry_types.add(data['geometry']['type'])
                        print(f"  Single feature: {data['geometry']['type']}")
                except json.JSONDecodeError as e:
                    print(f"  JSON decode error: {e}")
                    return 'Unknown'
        
        print(f"Found geometry types: {geometry_types}")
        print(f"Sample count: {sample_count}")
        
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
        
        print(f"Normalized types: {normalized_types}")
        
        # Return the primary geometry type
        if len(normalized_types) == 1:
            return list(normalized_types)[0]
        elif len(normalized_types) > 1:
            return 'Mixed'
        else:
            return 'Unknown'
            
    except Exception as e:
        print(f"Error: {e}")
        return 'Unknown'

# Test files
test_files = [
    Path("/Users/matthewheaton/GitHub/basemap/processing/data/GRID3_COD_health_facilities_v5_0.geojson"),
    Path("/Users/matthewheaton/GitHub/basemap/processing/data/infrastructure.geojsonseq"),
    Path("/Users/matthewheaton/GitHub/basemap/processing/data/land_use.geojsonseq"),
]

for test_file in test_files:
    print(f"\n{'='*50}")
    result = detect_geometry_type(test_file)
    print(f"RESULT: {result}")
    print('='*50)
