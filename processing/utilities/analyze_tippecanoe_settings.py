#!/usr/bin/env python3
"""
Tippecanoe Settings Comparison and Optimization Tool

This tool helps analyze and compare tippecanoe settings across different geometry types
and layer types to identify optimization opportunities.
"""

import json
import sys
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Set, Tuple

# Add processing directory to path
sys.path.insert(0, str(Path(__file__).parent))

from runCreateTiles import (
    detect_geometry_type, 
    get_layer_tippecanoe_settings,
    DATA_DIR,
    OVERTURE_DATA_DIR
)

def parse_tippecanoe_setting(setting: str) -> Tuple[str, str]:
    """Parse a tippecanoe setting into parameter and value"""
    if '=' in setting:
        param, value = setting.split('=', 1)
        return param.lstrip('-'), value
    else:
        return setting.lstrip('-'), 'enabled'

def analyze_settings_patterns():
    """Analyze patterns in tippecanoe settings across different layer types"""
    print("=== TIPPECANOE SETTINGS ANALYSIS ===\n")
    
    # Test cases for all layer types
    test_cases = [
        ('water', None, 'Water Features'),
        ('settlement-extents', None, 'Settlement Extents'),
        ('roads', None, 'Roads/Transportation'),
        ('places', None, 'Places/Points'),
        ('land_use', None, 'Base Polygons'),
        (None, 'point_file.geojson', 'Point Geometry (detected)'),
        (None, 'line_file.geojsonseq', 'LineString Geometry (detected)'),
        (None, 'polygon_file.geojsonseq', 'Polygon Geometry (detected)'),
        (None, 'mixed_file.geojson', 'Mixed Geometry (detected)'),
    ]
    
    settings_by_type = {}
    
    for layer_name, filename, description in test_cases:
        # Create a mock file for geometry detection if needed
        if filename and not layer_name:
            mock_geometry_type = None
            if 'point' in filename:
                mock_geometry_type = 'Point'
            elif 'line' in filename:
                mock_geometry_type = 'LineString'
            elif 'polygon' in filename:
                mock_geometry_type = 'Polygon'
            elif 'mixed' in filename:
                mock_geometry_type = 'Mixed'
            
            # Mock the detect_geometry_type function temporarily
            original_detect = detect_geometry_type
            detect_geometry_type.__globals__['detect_geometry_type'] = lambda x: mock_geometry_type
        
        try:
            settings = get_layer_tippecanoe_settings(layer_name, filename)
            settings_by_type[description] = settings
            print(f"**{description}:**")
            for setting in settings:
                print(f"  {setting}")
            print()
        except Exception as e:
            print(f"Error getting settings for {description}: {e}")
        
        # Restore original function if mocked
        if filename and not layer_name:
            detect_geometry_type.__globals__['detect_geometry_type'] = original_detect
    
    return settings_by_type

def compare_settings_parameters(settings_by_type: Dict[str, List[str]]):
    """Compare specific parameters across different layer types"""
    print("=== PARAMETER COMPARISON ===\n")
    
    # Parse all settings into parameter-value pairs
    parsed_settings = {}
    for layer_type, settings in settings_by_type.items():
        parsed_settings[layer_type] = {}
        for setting in settings:
            param, value = parse_tippecanoe_setting(setting)
            parsed_settings[layer_type][param] = value
    
    # Find all unique parameters
    all_parameters = set()
    for settings in parsed_settings.values():
        all_parameters.update(settings.keys())
    
    # Compare each parameter across layer types
    for param in sorted(all_parameters):
        print(f"**{param}:**")
        
        values_by_type = {}
        for layer_type, settings in parsed_settings.items():
            if param in settings:
                values_by_type[layer_type] = settings[param]
        
        if len(set(values_by_type.values())) > 1:  # Different values exist
            for layer_type, value in values_by_type.items():
                print(f"  {layer_type}: {value}")
        else:
            # All have same value
            common_value = list(values_by_type.values())[0] if values_by_type else "not set"
            layer_types = ", ".join(values_by_type.keys())
            print(f"  {common_value} (used by: {layer_types})")
        
        print()

def identify_optimization_opportunities(settings_by_type: Dict[str, List[str]]):
    """Identify potential optimization opportunities"""
    print("=== OPTIMIZATION OPPORTUNITIES ===\n")
    
    # Parse settings for analysis
    parsed_settings = {}
    for layer_type, settings in settings_by_type.items():
        parsed_settings[layer_type] = {}
        for setting in settings:
            param, value = parse_tippecanoe_setting(setting)
            parsed_settings[layer_type][param] = value
    
    # Opportunity 1: Inconsistent zoom ranges
    print("**1. Zoom Range Analysis:**")
    zoom_analysis = {}
    for layer_type, settings in parsed_settings.items():
        min_zoom = settings.get('minimum-zoom', 'default')
        max_zoom = settings.get('maximum-zoom', 'default')
        zoom_analysis[layer_type] = (min_zoom, max_zoom)
    
    for layer_type, (min_z, max_z) in zoom_analysis.items():
        print(f"  {layer_type}: min={min_z}, max={max_z}")
    print()
    
    # Opportunity 2: Drop rate variations
    print("**2. Drop Rate Analysis:**")
    drop_rates = {}
    for layer_type, settings in parsed_settings.items():
        drop_rate = settings.get('drop-rate', 'not set')
        drop_rates[layer_type] = drop_rate
    
    # Group by drop rate value
    rate_groups = defaultdict(list)
    for layer_type, rate in drop_rates.items():
        rate_groups[rate].append(layer_type)
    
    for rate, layer_types in rate_groups.items():
        print(f"  Drop rate {rate}: {', '.join(layer_types)}")
    print()
    
    # Opportunity 3: Buffer settings
    print("**3. Buffer Settings Analysis:**")
    buffer_settings = {}
    for layer_type, settings in parsed_settings.items():
        buffer_val = settings.get('buffer', 'uses base default (8)')
        buffer_settings[layer_type] = buffer_val
    
    for layer_type, buffer_val in buffer_settings.items():
        print(f"  {layer_type}: {buffer_val}")
    print()
    
    # Opportunity 4: Complexity analysis
    print("**4. Settings Complexity Analysis:**")
    complexity_scores = {}
    for layer_type, settings in settings_by_type.items():
        # Simple complexity score based on number of settings
        complexity_scores[layer_type] = len(settings)
    
    sorted_complexity = sorted(complexity_scores.items(), key=lambda x: x[1], reverse=True)
    for layer_type, score in sorted_complexity:
        print(f"  {layer_type}: {score} settings")
    print()

def generate_settings_matrix():
    """Generate a matrix view of all settings for easy comparison"""
    print("=== SETTINGS MATRIX ===\n")
    
    # Get all layer types and their settings
    layer_types = [
        ('water', None, 'Water'),
        ('settlement-extents', None, 'Settlement'),
        ('roads', None, 'Roads'),
        ('places', None, 'Places'),
        ('land_use', None, 'BasePolygon'),
    ]
    
    # Collect all settings
    all_settings = {}
    all_parameters = set()
    
    for layer_name, filename, short_name in layer_types:
        settings = get_layer_tippecanoe_settings(layer_name, filename)
        parsed = {}
        for setting in settings:
            param, value = parse_tippecanoe_setting(setting)
            parsed[param] = value
            all_parameters.add(param)
        all_settings[short_name] = parsed
    
    # Generate matrix
    print(f"{'Parameter':<25} | " + " | ".join(f"{name:<12}" for _, _, name in layer_types))
    print("-" * (25 + 3 + len(layer_types) * 15))
    
    for param in sorted(all_parameters):
        row = f"{param:<25} |"
        for _, _, short_name in layer_types:
            value = all_settings[short_name].get(param, "-")
            row += f" {value:<12} |"
        print(row)
    
    print()

def recommend_consolidation_opportunities():
    """Recommend opportunities for settings consolidation"""
    print("=== CONSOLIDATION RECOMMENDATIONS ===\n")
    
    # Get settings for analysis
    layer_types = [
        ('water', None, 'Water'),
        ('settlement-extents', None, 'Settlement'),
        ('roads', None, 'Roads'),
        ('places', None, 'Places'),
        ('land_use', None, 'BasePolygon'),
    ]
    
    all_settings = {}
    for layer_name, filename, short_name in layer_types:
        settings = get_layer_tippecanoe_settings(layer_name, filename)
        parsed = {}
        for setting in settings:
            param, value = parse_tippecanoe_setting(setting)
            parsed[param] = value
        all_settings[short_name] = parsed
    
    # Find common settings that could be moved to base
    parameter_usage = defaultdict(lambda: defaultdict(int))
    
    for layer_type, settings in all_settings.items():
        for param, value in settings.items():
            parameter_usage[param][value] += 1
    
    print("**Parameters used by multiple layer types (consolidation candidates):**")
    for param, value_counts in parameter_usage.items():
        if len(value_counts) == 1 and list(value_counts.values())[0] >= 3:
            # Same value used by 3+ layer types
            common_value = list(value_counts.keys())[0]
            count = list(value_counts.values())[0]
            print(f"  {param}={common_value} (used by {count} layer types)")
    
    print("\n**Parameters with conflicting values (review needed):**")
    for param, value_counts in parameter_usage.items():
        if len(value_counts) > 1:
            print(f"  {param}:")
            for value, count in value_counts.items():
                print(f"    {value}: {count} layer types")
    
    print()

def main():
    """Run the complete settings analysis suite"""
    print("Tippecanoe Settings Analysis Tool")
    print("=" * 50)
    
    # Run all analyses
    settings_by_type = analyze_settings_patterns()
    compare_settings_parameters(settings_by_type)
    identify_optimization_opportunities(settings_by_type)
    generate_settings_matrix()
    recommend_consolidation_opportunities()
    
    # Save analysis results
    results_file = Path(__file__).parent / 'settings_analysis.json'
    with open(results_file, 'w') as f:
        json.dump({
            'settings_by_type': settings_by_type,
            'analysis_timestamp': str(Path(__file__).stat().st_mtime),
        }, f, indent=2)
    
    print(f"Analysis results saved to: {results_file}")

if __name__ == "__main__":
    main()
