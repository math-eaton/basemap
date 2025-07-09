#!/usr/bin/env python3
"""
Utility to validate and fix polygon winding order in GeoJSON files.
"""

import json
import os
from shapely.geometry import shape, Polygon, MultiPolygon

def validate_and_fix_winding(file_path):
    """
    Validate and fix polygon winding order in a GeoJSON file.

    Args:
        file_path (str): Path to the GeoJSON file.

    Returns:
        None
    """
    print(f"Validating and fixing winding order for {file_path}...")

    # Load GeoJSON data
    with open(file_path, 'r') as f:
        data = json.load(f)

    fixed_features = []

    for feature in data.get('features', []):
        geometry = feature.get('geometry')
        if not geometry:
            fixed_features.append(feature)
            continue

        geom = shape(geometry)

        # Fix winding order for polygons
        if isinstance(geom, Polygon):
            if not geom.exterior.is_ccw:
                geom = Polygon(list(geom.exterior.coords)[::-1], [list(interior.coords) for interior in geom.interiors])
        elif isinstance(geom, MultiPolygon):
            fixed_polygons = []
            for poly in geom.geoms:
                if not poly.exterior.is_ccw:
                    fixed_polygons.append(Polygon(list(poly.exterior.coords)[::-1], [list(interior.coords) for interior in poly.interiors]))
                else:
                    fixed_polygons.append(poly)
            geom = MultiPolygon(fixed_polygons)

        # Update feature geometry
        feature['geometry'] = geom.__geo_interface__
        fixed_features.append(feature)

    # Save fixed GeoJSON
    output_path = file_path.replace('.geojson', '_fixed.geojson')
    with open(output_path, 'w') as f:
        json.dump({"type": "FeatureCollection", "features": fixed_features}, f, indent=2)

    print(f"Fixed GeoJSON saved to {output_path}")

def validate_and_fix_winding_in_directory(directory_path):
    """
    Validate and fix polygon winding order for all GeoJSON and GeoJSONSeq files in a directory.

    Args:
        directory_path (str): Path to the directory containing GeoJSON files.

    Returns:
        None
    """
    print(f"Validating and fixing winding order for files in {directory_path}...")

    if not os.path.exists(directory_path):
        print(f"Error: Directory {directory_path} does not exist.")
        return

    geojson_files = [
        f for f in os.listdir(directory_path)
        if f.endswith('.geojson') or f.endswith('.geojsonseq')
    ]

    if not geojson_files:
        print("No GeoJSON or GeoJSONSeq files found in the directory.")
        return

    for file in geojson_files:
        file_path = os.path.join(directory_path, file)
        print(f"Processing {file}...")
        try:
            validate_and_fix_winding(file_path)
        except Exception as e:
            print(f"Error processing {file}: {e}")

    print("Validation and fixing complete for all files in the directory.")

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python validate_polygon_winding.py <path_to_directory>")
        sys.exit(1)

    directory_path = sys.argv[1]
    validate_and_fix_winding_in_directory(directory_path)
