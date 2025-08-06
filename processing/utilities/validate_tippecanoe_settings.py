#!/usr/bin/env python3
"""
Validation script for tippecanoe settings by geometry type.

This script provides comprehensive testing and validation of the geometry detection
and tippecanoe settings selection functionality.
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Set

# Add the processing directory to the path so we can import from runCreateTiles
sys.path.insert(0, str(Path(__file__).parent))

from runCreateTiles import (
    detect_geometry_type, 
    get_layer_tippecanoe_settings,
    DATA_DIR,
    OVERTURE_DATA_DIR
)

def validate_geometry_detection():
    """Test geometry detection on all available data files"""
    print("=== GEOMETRY DETECTION VALIDATION ===")
    
    test_results = []
    data_dirs = [DATA_DIR, OVERTURE_DATA_DIR]
    
    for data_dir in data_dirs:
        if not data_dir.exists():
            print(f"Data directory {data_dir} does not exist, skipping...")
            continue
            
        print(f"\nTesting files in: {data_dir}")
        
        for file_path in data_dir.glob("*.geojson*"):
            try:
                geometry_type = detect_geometry_type(file_path)
                test_results.append({
                    'file': file_path.name,
                    'path': str(file_path),
                    'detected_type': geometry_type,
                    'status': 'success'
                })
                print(f"  ✓ {file_path.name}: {geometry_type}")
            except Exception as e:
                test_results.append({
                    'file': file_path.name,
                    'path': str(file_path),
                    'detected_type': None,
                    'status': 'error',
                    'error': str(e)
                })
                print(f"  ✗ {file_path.name}: ERROR - {e}")
    
    return test_results

def validate_tippecanoe_settings():
    """Test tippecanoe settings generation for different layer types"""
    print("\n=== TIPPECANOE SETTINGS VALIDATION ===")
    
    # Test predefined layer types
    test_cases = [
        ('water', None),
        ('settlement-extents', None),
        ('roads', None),
        ('places', None),
        ('land_use', None),
        (None, 'water_features.geojson'),
        (None, 'settlement_extents.geojsonseq'),
        (None, 'roads_network.geojsonseq'),
        (None, 'health_facilities.geojson'),
        (None, 'land_use_polygons.geojsonseq'),
    ]
    
    settings_results = []
    
    for layer_name, filename in test_cases:
        try:
            settings = get_layer_tippecanoe_settings(layer_name, filename)
            settings_results.append({
                'layer_name': layer_name,
                'filename': filename,
                'settings': settings,
                'status': 'success'
            })
            
            identifier = layer_name if layer_name else filename
            print(f"  ✓ {identifier}:")
            for setting in settings[:3]:  # Show first 3 settings
                print(f"    {setting}")
            if len(settings) > 3:
                print(f"    ... and {len(settings) - 3} more settings")
                
        except Exception as e:
            settings_results.append({
                'layer_name': layer_name,
                'filename': filename,
                'settings': None,
                'status': 'error',
                'error': str(e)
            })
            identifier = layer_name if layer_name else filename
            print(f"  ✗ {identifier}: ERROR - {e}")
    
    return settings_results

def validate_geometry_to_settings_mapping():
    """Test the mapping from detected geometry types to appropriate settings"""
    print("\n=== GEOMETRY-TO-SETTINGS MAPPING VALIDATION ===")
    
    # Find actual data files and test the full pipeline
    test_files = []
    data_dirs = [DATA_DIR, OVERTURE_DATA_DIR]
    
    for data_dir in data_dirs:
        if data_dir.exists():
            for file_path in list(data_dir.glob("*.geojson*"))[:5]:  # Test first 5 files
                test_files.append(file_path)
    
    mapping_results = []
    
    for file_path in test_files:
        try:
            # Detect geometry type
            geometry_type = detect_geometry_type(file_path)
            
            # Get settings for this file (will use geometry-based defaults)
            settings = get_layer_tippecanoe_settings(None, file_path)
            
            # Validate that settings are appropriate for geometry type
            is_valid = validate_settings_for_geometry(geometry_type, settings)
            
            mapping_results.append({
                'file': file_path.name,
                'geometry_type': geometry_type,
                'settings_count': len(settings),
                'is_valid': is_valid,
                'status': 'success'
            })
            
            status_icon = "✓" if is_valid else "⚠"
            print(f"  {status_icon} {file_path.name}: {geometry_type} → {len(settings)} settings")
            
        except Exception as e:
            mapping_results.append({
                'file': file_path.name,
                'geometry_type': None,
                'settings_count': 0,
                'is_valid': False,
                'status': 'error',
                'error': str(e)
            })
            print(f"  ✗ {file_path.name}: ERROR - {e}")
    
    return mapping_results

def validate_settings_for_geometry(geometry_type: str, settings: List[str]) -> bool:
    """Validate that tippecanoe settings are appropriate for the geometry type"""
    
    # Convert settings list to a set for easier checking
    settings_str = ' '.join(settings)
    
    if geometry_type == 'Point':
        # Points should have cluster-distance and appropriate zoom settings
        return '--cluster-distance=' in settings_str
    
    elif geometry_type == 'LineString':
        # Lines should have no-line-simplification or appropriate buffer
        return '--no-line-simplification' in settings_str or '--buffer=' in settings_str
    
    elif geometry_type == 'Polygon':
        # Polygons should have polygon-specific settings
        return ('--simplification=' in settings_str and 
                '--drop-rate=' in settings_str)
    
    else:  # Mixed or Unknown
        # Should have conservative settings
        return '--drop-rate=' in settings_str
    
def generate_validation_report(geometry_results, settings_results, mapping_results):
    """Generate a comprehensive validation report"""
    print("\n=== VALIDATION SUMMARY ===")
    
    # Geometry detection summary
    geometry_success = sum(1 for r in geometry_results if r['status'] == 'success')
    geometry_total = len(geometry_results)
    print(f"Geometry Detection: {geometry_success}/{geometry_total} successful")
    
    # Settings generation summary
    settings_success = sum(1 for r in settings_results if r['status'] == 'success')
    settings_total = len(settings_results)
    print(f"Settings Generation: {settings_success}/{settings_total} successful")
    
    # Mapping validation summary
    mapping_success = sum(1 for r in mapping_results if r['status'] == 'success' and r['is_valid'])
    mapping_total = len([r for r in mapping_results if r['status'] == 'success'])
    print(f"Geometry-Settings Mapping: {mapping_success}/{mapping_total} valid")
    
    # Overall assessment
    total_tests = geometry_total + settings_total + mapping_total
    total_success = geometry_success + settings_success + mapping_success
    success_rate = (total_success / total_tests) * 100 if total_tests > 0 else 0
    
    print(f"\nOverall Success Rate: {total_success}/{total_tests} ({success_rate:.1f}%)")
    
    if success_rate >= 90:
        print("✅ VALIDATION PASSED: Tippecanoe settings system is working correctly")
    elif success_rate >= 75:
        print("⚠️  VALIDATION WARNING: Some issues detected, review recommended")
    else:
        print("❌ VALIDATION FAILED: Significant issues detected, fixes required")
    
    return {
        'geometry_detection': {'success': geometry_success, 'total': geometry_total},
        'settings_generation': {'success': settings_success, 'total': settings_total},
        'mapping_validation': {'success': mapping_success, 'total': mapping_total},
        'overall_success_rate': success_rate
    }

def main():
    """Run the complete validation suite"""
    print("Tippecanoe Settings Validation Suite")
    print("=" * 50)
    
    # Run all validation tests
    geometry_results = validate_geometry_detection()
    settings_results = validate_tippecanoe_settings()
    mapping_results = validate_geometry_to_settings_mapping()
    
    # Generate summary report
    report = generate_validation_report(geometry_results, settings_results, mapping_results)
    
    # Save detailed results to JSON for further analysis if needed
    results_file = Path(__file__).parent / 'validation_results.json'
    with open(results_file, 'w') as f:
        json.dump({
            'geometry_detection': geometry_results,
            'settings_generation': settings_results,
            'mapping_validation': mapping_results,
            'summary': report
        }, f, indent=2)
    
    print(f"\nDetailed results saved to: {results_file}")

if __name__ == "__main__":
    main()
