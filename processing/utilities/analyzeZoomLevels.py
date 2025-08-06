#!/usr/bin/env python3
"""
MapLibre Style Zoom Level Analyzer

This script analyzes MapLibre/Mapbox GL JS style specifications to summarize
tile layers by zoom levels, helping understand scale categorization in vector
tile basemaps.

Usage:
    python analyzeZoomLevels.py <style.json>
    python analyzeZoomLevels.py --help
"""

import json
import argparse
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
import sys


def load_style_json(filepath: str) -> Dict:
    """Load and parse MapLibre style JSON file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in '{filepath}': {e}")
        sys.exit(1)


def extract_zoom_info(layer: Dict) -> Tuple[Optional[int], Optional[int]]:
    """Extract minzoom and maxzoom from a layer."""
    minzoom = layer.get('minzoom')
    maxzoom = layer.get('maxzoom')
    return minzoom, maxzoom


def categorize_by_scale(minzoom: Optional[int], maxzoom: Optional[int]) -> str:
    """
    Categorize layers by scale based on zoom levels.
    
    Common scale categories:
    - Small scale (global/regional): 0-7
    - Medium scale (local/city): 8-11  
    - Large scale (neighborhood/building): 12+
    """
    if minzoom is None and maxzoom is None:
        return "all_scales"
    
    effective_min = minzoom if minzoom is not None else 0
    effective_max = maxzoom if maxzoom is not None else 22
    
    if effective_max <= 7:
        return "small_scale"
    elif effective_min >= 12:
        return "large_scale"
    elif effective_min >= 8 and effective_max <= 11:
        return "medium_scale"
    elif effective_min <= 7 and effective_max >= 12:
        return "multi_scale"
    elif effective_min <= 7 and effective_max <= 11:
        return "small_to_medium"
    elif effective_min >= 8 and effective_max >= 12:
        return "medium_to_large"
    else:
        return "other"


def analyze_layers(style_data: Dict) -> Dict:
    """Analyze all layers in the style and categorize by zoom levels."""
    
    layers = style_data.get('layers', [])
    
    # Data structures for analysis
    zoom_analysis = {
        'layer_count': len(layers),
        'zoom_ranges': defaultdict(list),
        'scale_categories': defaultdict(list),
        'layer_types': defaultdict(list),
        'source_usage': defaultdict(list),
        'zoom_distribution': defaultdict(int)
    }
    
    for layer in layers:
        layer_id = layer.get('id', 'unknown')
        layer_type = layer.get('type', 'unknown')
        source = layer.get('source', 'unknown')
        source_layer = layer.get('source-layer', 'unknown')
        
        minzoom, maxzoom = extract_zoom_info(layer)
        scale_category = categorize_by_scale(minzoom, maxzoom)
        
        # Store layer info
        layer_info = {
            'id': layer_id,
            'type': layer_type,
            'source': source,
            'source-layer': source_layer,
            'minzoom': minzoom,
            'maxzoom': maxzoom,
            'scale_category': scale_category
        }
        
        # Categorize by zoom ranges
        zoom_key = f"{minzoom}-{maxzoom}"
        zoom_analysis['zoom_ranges'][zoom_key].append(layer_info)
        
        # Categorize by scale
        zoom_analysis['scale_categories'][scale_category].append(layer_info)
        
        # Categorize by layer type
        zoom_analysis['layer_types'][layer_type].append(layer_info)
        
        # Categorize by source
        zoom_analysis['source_usage'][source].append(layer_info)
        
        # Count zoom distribution
        for zoom in range(minzoom or 0, (maxzoom or 22) + 1):
            zoom_analysis['zoom_distribution'][zoom] += 1
    
    return zoom_analysis


def print_summary(analysis: Dict, style_data: Dict):
    """Print a comprehensive summary of the zoom level analysis."""
    
    print("=" * 80)
    print("MAPLIBRE STYLE ZOOM LEVEL ANALYSIS")
    print("=" * 80)
    
    # Basic info
    print(f"\nStyle Name: {style_data.get('name', 'Unknown')}")
    print(f"Total Layers: {analysis['layer_count']}")
    print(f"Sources: {len(style_data.get('sources', {}))}")
    
    # Scale categories summary
    print("\n" + "=" * 50)
    print("SCALE CATEGORIES SUMMARY")
    print("=" * 50)
    
    scale_descriptions = {
        'small_scale': 'Small Scale (Global/Regional, z0-7)',
        'medium_scale': 'Medium Scale (City/Local, z8-11)',
        'large_scale': 'Large Scale (Neighborhood/Building, z12+)',
        'small_to_medium': 'Small to Medium Scale (z0-11)',
        'medium_to_large': 'Medium to Large Scale (z8+)',
        'multi_scale': 'Multi-Scale (z0-12+)',
        'all_scales': 'All Scales (no zoom limits)',
        'other': 'Other'
    }
    
    for category, layers in analysis['scale_categories'].items():
        print(f"\n{scale_descriptions.get(category, category)}: {len(layers)} layers")
        
        # Group by source-layer for better organization
        source_layers = defaultdict(list)
        for layer in layers:
            source_layers[layer['source-layer']].append(layer)
        
        for source_layer, layer_list in sorted(source_layers.items()):
            print(f"  {source_layer}:")
            for layer in sorted(layer_list, key=lambda x: x['minzoom'] or 0):
                zoom_range = f"z{layer['minzoom'] or 0}-{layer['maxzoom'] or '∞'}"
                print(f"    - {layer['id']} ({layer['type']}) [{zoom_range}]")
    
    # Zoom distribution
    print("\n" + "=" * 50)
    print("ZOOM LEVEL DISTRIBUTION")
    print("=" * 50)
    
    print("\nLayers active at each zoom level:")
    for zoom in sorted(analysis['zoom_distribution'].keys()):
        count = analysis['zoom_distribution'][zoom]
        bar = "█" * min(count // 2, 50)  # Visual bar
        print(f"z{zoom:2d}: {count:3d} layers {bar}")
    
    # Layer types summary
    print("\n" + "=" * 50)
    print("LAYER TYPES SUMMARY")
    print("=" * 50)
    
    for layer_type, layers in sorted(analysis['layer_types'].items()):
        print(f"\n{layer_type}: {len(layers)} layers")
        
        # Show zoom range distribution for this type
        zoom_ranges = {}
        for layer in layers:
            zoom_key = f"{layer['minzoom'] or 0}-{layer['maxzoom'] or '∞'}"
            zoom_ranges[zoom_key] = zoom_ranges.get(zoom_key, 0) + 1
        
        for zoom_range, count in sorted(zoom_ranges.items()):
            print(f"  z{zoom_range}: {count} layers")
    
    # Most common zoom ranges
    print("\n" + "=" * 50)
    print("MOST COMMON ZOOM RANGES")
    print("=" * 50)
    
    zoom_range_counts = {}
    for zoom_range, layers in analysis['zoom_ranges'].items():
        zoom_range_counts[zoom_range] = len(layers)
    
    for zoom_range, count in sorted(zoom_range_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"z{zoom_range}: {count} layers")
    
    # Recommendations
    print("\n" + "=" * 50)
    print("SCALE GENERALIZATION RECOMMENDATIONS")
    print("=" * 50)
    
    print("\nBased on this ESRI basemap analysis:")
    print("• Small Scale (z0-7): Use for global/regional features")
    print("• Medium Scale (z8-11): Use for city/local features") 
    print("• Large Scale (z12+): Use for neighborhood/building details")
    print("\nConsider these zoom ranges when generalizing your input data:")
    
    # Find the most common patterns
    small_layers = len(analysis['scale_categories']['small_scale']) + len(analysis['scale_categories']['small_to_medium'])
    medium_layers = len(analysis['scale_categories']['medium_scale']) + len(analysis['scale_categories']['medium_to_large'])
    large_layers = len(analysis['scale_categories']['large_scale'])
    
    print(f"• Small scale emphasis: {small_layers} layers")
    print(f"• Medium scale emphasis: {medium_layers} layers")
    print(f"• Large scale emphasis: {large_layers} layers")


def main():
    """Main function to run the analysis."""
    parser = argparse.ArgumentParser(
        description='Analyze MapLibre style JSON to understand zoom level patterns',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python analyzeZoomLevels.py style.json
    python analyzeZoomLevels.py esri_basemap.json
    python analyzeZoomLevels.py --detailed style.json
        """
    )
    
    parser.add_argument('style_file', help='Path to MapLibre style JSON file')
    parser.add_argument('--detailed', action='store_true', 
                       help='Show detailed layer information')
    parser.add_argument('--export', type=str, 
                       help='Export analysis to JSON file')
    
    args = parser.parse_args()
    
    # Load and analyze the style
    style_data = load_style_json(args.style_file)
    analysis = analyze_layers(style_data)
    
    # Print summary
    print_summary(analysis, style_data)
    
    # Export if requested
    if args.export:
        with open(args.export, 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        print(f"\nAnalysis exported to: {args.export}")


if __name__ == "__main__":
    main()