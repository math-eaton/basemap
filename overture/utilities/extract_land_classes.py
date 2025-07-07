#!/usr/bin/env python3
"""
Extract land classes from Overture Maps GeoJSON data and create detailed color schemes
for cartographic visualization of different land cover types.
"""

import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from collections import defaultdict, Counter
import seaborn as sns

class LandClassExtractor:
    def __init__(self, geojson_file):
        self.geojson_file = geojson_file
        self.land_classes = defaultdict(list)
        self.class_counts = Counter()
        self.subtype_counts = Counter()
        
        # Define comprehensive color schemes for each land class
        self.color_schemes = {
            # Forest and woodland
            'wood': {
                'primary': '#2D5016',      # Dark forest green
                'secondary': '#4A7C59',    # Medium forest green
                'tertiary': '#6B8E5A',     # Light forest green
                'description': 'Wooded areas, forests, and tree coverage'
            },
            'forest': {
                'primary': '#1B4D3E',      # Deep forest green
                'secondary': '#2F7D32',    # Forest service green
                'tertiary': '#388E3C',     # Bright forest green
                'description': 'Managed forests and forest reserves'
            },
            'tree': {
                'primary': '#4CAF50',      # Tree green
                'secondary': '#66BB6A',    # Light tree green
                'tertiary': '#81C784',     # Very light tree green
                'description': 'Individual trees and small tree clusters'
            },
            'tree_row': {
                'primary': '#558B2F',      # Row tree green
                'secondary': '#689F38',    # Light row green
                'tertiary': '#7CB342',     # Bright row green
                'description': 'Tree rows, windbreaks, and linear tree features'
            },
            
            # Shrubland and scrub
            'scrub': {
                'primary': '#8BC34A',      # Scrub green
                'secondary': '#9CCC65',    # Light scrub
                'tertiary': '#AED581',     # Very light scrub
                'description': 'Scrubland, bushes, and low vegetation'
            },
            'shrub': {
                'primary': '#689F38',      # Shrub green
                'secondary': '#7CB342',    # Medium shrub
                'tertiary': '#8BC34A',     # Light shrub
                'description': 'Shrub areas and bushy vegetation'
            },
            
            # Grassland and meadows
            'grassland': {
                'primary': '#8BC34A',      # Grassland green
                'secondary': '#9CCC65',    # Light grass
                'tertiary': '#C5E1A5',     # Very light grass
                'description': 'Natural grasslands and prairies'
            },
            'grass': {
                'primary': '#7CB342',      # Grass green
                'secondary': '#8BC34A',    # Medium grass
                'tertiary': '#9CCC65',     # Light grass
                'description': 'Grass areas and turf'
            },
            'meadow': {
                'primary': '#9CCC65',      # Meadow green
                'secondary': '#AED581',    # Light meadow
                'tertiary': '#C5E1A5',     # Very light meadow
                'description': 'Meadows and open grassy areas'
            },
            'heath': {
                'primary': '#827717',      # Heath brown-green
                'secondary': '#9E9D24',    # Medium heath
                'tertiary': '#AFB42B',     # Light heath
                'description': 'Heathland and moorland'
            },
            
            # Wetlands
            'wetland': {
                'primary': '#006064',      # Dark wetland blue-green
                'secondary': '#00838F',    # Medium wetland
                'tertiary': '#0097A7',     # Light wetland
                'description': 'Wetlands, swamps, and marshy areas'
            },
            
            # Water and coastal features
            'beach': {
                'primary': '#FFF176',      # Sand yellow
                'secondary': '#FFEB3B',    # Bright sand
                'tertiary': '#F9FBE7',     # Very light sand
                'description': 'Beaches and sandy shores'
            },
            'sand': {
                'primary': '#F57F17',      # Sand orange
                'secondary': '#FF8F00',    # Bright sand orange
                'tertiary': '#FFA000',     # Light sand orange
                'description': 'Sandy areas and dunes'
            },
            
            # Topographic features
            'peak': {
                'primary': '#795548',      # Mountain brown
                'secondary': '#8D6E63',    # Light mountain brown
                'tertiary': '#A1887F',     # Very light mountain brown
                'description': 'Mountain peaks and high elevation points'
            },
            'cliff': {
                'primary': '#5D4037',      # Dark cliff brown
                'secondary': '#6D4C41',    # Medium cliff brown
                'tertiary': '#8D6E63',     # Light cliff brown
                'description': 'Cliffs and steep rock faces'
            },
            'bare_rock': {
                'primary': '#757575',      # Rock gray
                'secondary': '#9E9E9E',    # Light rock gray
                'tertiary': '#BDBDBD',     # Very light rock gray
                'description': 'Exposed rock surfaces'
            },
            'stone': {
                'primary': '#616161',      # Stone gray
                'secondary': '#757575',    # Light stone gray
                'tertiary': '#9E9E9E',     # Very light stone gray
                'description': 'Stone formations and rocky areas'
            },
            
            # Land features
            'island': {
                'primary': '#D32F2F',      # Island red
                'secondary': '#F44336',    # Light island red
                'tertiary': '#EF5350',     # Very light island red
                'description': 'Islands and large land masses surrounded by water'
            },
            'islet': {
                'primary': '#C62828',      # Islet dark red
                'secondary': '#D32F2F',    # Medium islet red
                'tertiary': '#F44336',     # Light islet red
                'description': 'Small islands and rocky outcrops'
            }
        }
    
    def extract_land_classes(self):
        """Extract land classes from the GeoJSONSeq file"""
        print(f"Extracting land classes from {self.geojson_file}...")
        
        with open(self.geojson_file, 'r') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    feature = json.loads(line.strip())
                    if feature.get('type') == 'Feature':
                        properties = feature.get('properties', {})
                        land_class = properties.get('class')
                        subtype = properties.get('subtype')
                        name = properties.get('name')
                        geometry_type = feature.get('geometry', {}).get('type')
                        
                        if land_class:
                            self.class_counts[land_class] += 1
                            self.land_classes[land_class].append({
                                'subtype': subtype,
                                'name': name,
                                'geometry_type': geometry_type,
                                'line_number': line_num
                            })
                        
                        if subtype:
                            self.subtype_counts[subtype] += 1
                            
                except json.JSONDecodeError as e:
                    print(f"Error parsing line {line_num}: {e}")
                    continue
                except Exception as e:
                    print(f"Unexpected error on line {line_num}: {e}")
                    continue
        
        print(f"Extracted {len(self.class_counts)} unique land classes")
        print(f"Extracted {len(self.subtype_counts)} unique subtypes")
        
    def generate_summary_report(self):
        """Generate a comprehensive summary report of land classes"""
        print("\n" + "="*80)
        print("LAND CLASS EXTRACTION SUMMARY REPORT")
        print("="*80)
        
        # Class statistics
        print(f"\nTOTAL FEATURES PROCESSED: {sum(self.class_counts.values()):,}")
        print(f"UNIQUE LAND CLASSES: {len(self.class_counts)}")
        print(f"UNIQUE SUBTYPES: {len(self.subtype_counts)}")
        
        # Top land classes
        print(f"\nTOP 10 LAND CLASSES BY FREQUENCY:")
        print("-" * 50)
        for class_name, count in self.class_counts.most_common(10):
            percentage = (count / sum(self.class_counts.values())) * 100
            color_info = self.color_schemes.get(class_name, {})
            primary_color = color_info.get('primary', 'N/A')
            print(f"{class_name:15} | {count:6,} ({percentage:5.1f}%) | Color: {primary_color}")
        
        # Detailed class information
        print(f"\nDETAILED LAND CLASS INFORMATION:")
        print("-" * 80)
        for class_name in sorted(self.class_counts.keys()):
            count = self.class_counts[class_name]
            percentage = (count / sum(self.class_counts.values())) * 100
            
            # Get color scheme info
            color_info = self.color_schemes.get(class_name, {
                'primary': '#808080',
                'secondary': '#A0A0A0', 
                'tertiary': '#C0C0C0',
                'description': 'No description available'
            })
            
            print(f"\n{class_name.upper()}:")
            print(f"  Count: {count:,} features ({percentage:.1f}%)")
            print(f"  Description: {color_info['description']}")
            print(f"  Primary Color: {color_info['primary']}")
            print(f"  Secondary Color: {color_info['secondary']}")
            print(f"  Tertiary Color: {color_info['tertiary']}")
            
            # Show sample names if available
            examples = [item for item in self.land_classes[class_name] if item['name']]
            if examples:
                sample_names = list(set([item['name'] for item in examples[:5]]))
                print(f"  Example names: {', '.join(sample_names[:3])}")
    
    def create_color_palette_visualization(self):
        """Create a visual representation of the color schemes"""
        fig, axes = plt.subplots(figsize=(16, 12))
        
        # Prepare data for visualization
        classes = sorted(self.class_counts.keys())
        y_positions = range(len(classes))
        
        # Create horizontal bars for each class
        for i, class_name in enumerate(classes):
            count = self.class_counts[class_name]
            color_info = self.color_schemes.get(class_name, {'primary': '#808080'})
            
            # Draw the bar
            axes.barh(i, count, color=color_info['primary'], alpha=0.8, height=0.6)
            
            # Add text labels
            axes.text(count + max(self.class_counts.values()) * 0.01, i, 
                     f"{class_name} ({count:,})", 
                     va='center', fontsize=10)
        
        # Customize the plot
        axes.set_yticks(y_positions)
        axes.set_yticklabels(classes)
        axes.set_xlabel('Number of Features')
        axes.set_title('Land Classes by Frequency with Color Scheme\n(St. Lawrence River Region)', 
                      fontsize=14, fontweight='bold')
        axes.grid(axis='x', alpha=0.3)
        
        # Add color legend
        legend_elements = []
        for class_name in classes[:10]:  # Top 10 for legend
            color_info = self.color_schemes.get(class_name, {'primary': '#808080'})
            legend_elements.append(plt.Rectangle((0,0),1,1, 
                                               facecolor=color_info['primary'], 
                                               label=class_name))
        
        axes.legend(handles=legend_elements, loc='lower right', fontsize=8)
        
        plt.tight_layout()
        plt.savefig('/Users/matthewheaton/GitHub/0_CIESIN/GRID3_mapProduction/scripts/vectorTiling/overture/land_classes_visualization.png', 
                   dpi=300, bbox_inches='tight')
        print(f"\nColor palette visualization saved to: land_classes_visualization.png")
    
    def export_color_schemes(self):
        """Export color schemes to various formats for use in mapping software"""
        
        # Export to JSON for web mapping
        json_output = {
            'metadata': {
                'title': 'St. Lawrence Land Cover Color Schemes',
                'description': 'Color schemes for Overture Maps land cover data',
                'total_classes': len(self.class_counts),
                'total_features': sum(self.class_counts.values())
            },
            'color_schemes': {}
        }
        
        for class_name, color_info in self.color_schemes.items():
            if class_name in self.class_counts:
                json_output['color_schemes'][class_name] = {
                    'colors': {
                        'primary': color_info['primary'],
                        'secondary': color_info['secondary'],
                        'tertiary': color_info['tertiary']
                    },
                    'description': color_info['description'],
                    'feature_count': self.class_counts[class_name],
                    'percentage': round((self.class_counts[class_name] / sum(self.class_counts.values())) * 100, 2)
                }
        
        json_file = '/Users/matthewheaton/GitHub/0_CIESIN/GRID3_mapProduction/scripts/vectorTiling/overture/land_class_colors.json'
        with open(json_file, 'w') as f:
            json.dump(json_output, f, indent=2)
        print(f"Color schemes exported to JSON: {json_file}")
        
        # Export to CSV for spreadsheet use
        csv_data = []
        for class_name, color_info in self.color_schemes.items():
            if class_name in self.class_counts:
                csv_data.append({
                    'land_class': class_name,
                    'primary_color': color_info['primary'],
                    'secondary_color': color_info['secondary'],
                    'tertiary_color': color_info['tertiary'],
                    'description': color_info['description'],
                    'feature_count': self.class_counts[class_name],
                    'percentage': round((self.class_counts[class_name] / sum(self.class_counts.values())) * 100, 2)
                })
        
        df = pd.DataFrame(csv_data)
        df = df.sort_values('feature_count', ascending=False)
        csv_file = '/Users/matthewheaton/GitHub/0_CIESIN/GRID3_mapProduction/scripts/vectorTiling/overture/land_class_colors.csv'
        df.to_csv(csv_file, index=False)
        print(f"Color schemes exported to CSV: {csv_file}")
        
        # Export ArcGIS style format (simplified)
        arcgis_output = []
        for class_name, color_info in self.color_schemes.items():
            if class_name in self.class_counts:
                # Convert hex to RGB
                hex_color = color_info['primary'].lstrip('#')
                rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
                
                arcgis_output.append({
                    'value': class_name,
                    'label': class_name.replace('_', ' ').title(),
                    'description': color_info['description'],
                    'symbol': {
                        'type': 'simple-fill',
                        'color': rgb,
                        'outline': {'color': [128, 128, 128], 'width': 0.5}
                    }
                })
        
        arcgis_file = '/Users/matthewheaton/GitHub/0_CIESIN/GRID3_mapProduction/scripts/vectorTiling/overture/land_class_arcgis_symbology.json'
        with open(arcgis_file, 'w') as f:
            json.dump({'symbology': arcgis_output}, f, indent=2)
        print(f"ArcGIS symbology exported to: {arcgis_file}")
    
    def create_color_swatch_image(self):
        """Create a color swatch image showing all color schemes"""
        # Filter to only classes present in the data
        present_classes = [cls for cls in sorted(self.class_counts.keys()) 
                          if cls in self.color_schemes]
        
        n_classes = len(present_classes)
        fig, ax = plt.subplots(figsize=(12, n_classes * 0.8))
        
        y_pos = 0
        for class_name in present_classes:
            color_info = self.color_schemes[class_name]
            count = self.class_counts[class_name]
            percentage = (count / sum(self.class_counts.values())) * 100
            
            # Draw color swatches
            rect1 = plt.Rectangle((0, y_pos), 1, 0.8, 
                                facecolor=color_info['primary'], alpha=0.9)
            rect2 = plt.Rectangle((1, y_pos), 1, 0.8, 
                                facecolor=color_info['secondary'], alpha=0.9)
            rect3 = plt.Rectangle((2, y_pos), 1, 0.8, 
                                facecolor=color_info['tertiary'], alpha=0.9)
            
            ax.add_patch(rect1)
            ax.add_patch(rect2)
            ax.add_patch(rect3)
            
            # Add labels
            ax.text(3.2, y_pos + 0.4, 
                   f"{class_name.replace('_', ' ').title()}", 
                   va='center', fontsize=11, fontweight='bold')
            ax.text(3.2, y_pos + 0.1, 
                   f"{count:,} features ({percentage:.1f}%)", 
                   va='center', fontsize=9, alpha=0.7)
            
            # Add color hex codes
            ax.text(0.5, y_pos + 0.4, color_info['primary'], 
                   va='center', ha='center', fontsize=8, 
                   color='white' if sum(int(color_info['primary'][i:i+2], 16) for i in (1, 3, 5)) < 384 else 'black')
            ax.text(1.5, y_pos + 0.4, color_info['secondary'], 
                   va='center', ha='center', fontsize=8,
                   color='white' if sum(int(color_info['secondary'][i:i+2], 16) for i in (1, 3, 5)) < 384 else 'black')
            ax.text(2.5, y_pos + 0.4, color_info['tertiary'], 
                   va='center', ha='center', fontsize=8,
                   color='white' if sum(int(color_info['tertiary'][i:i+2], 16) for i in (1, 3, 5)) < 384 else 'black')
            
            y_pos += 1
        
        # Customize the plot
        ax.set_xlim(0, 8)
        ax.set_ylim(0, n_classes)
        ax.set_aspect('equal')
        ax.axis('off')
        
        # Add title and headers
        ax.text(1.5, n_classes + 0.5, 'Land Cover Color Schemes', 
               fontsize=16, fontweight='bold', ha='center')
        ax.text(0.5, n_classes + 0.2, 'Primary', fontsize=10, ha='center', fontweight='bold')
        ax.text(1.5, n_classes + 0.2, 'Secondary', fontsize=10, ha='center', fontweight='bold')
        ax.text(2.5, n_classes + 0.2, 'Tertiary', fontsize=10, ha='center', fontweight='bold')
        
        plt.tight_layout()
        swatch_file = '/Users/matthewheaton/GitHub/0_CIESIN/GRID3_mapProduction/scripts/vectorTiling/overture/land_class_color_swatches.png'
        plt.savefig(swatch_file, dpi=300, bbox_inches='tight')
        print(f"Color swatch image saved to: {swatch_file}")

def main():
    """Main execution function"""
    geojson_file = "/Users/matthewheaton/GitHub/0_CIESIN/GRID3_mapProduction/scripts/vectorTiling/overture/data/st_lawrence_land.geojsonseq"
    
    # Create extractor instance
    extractor = LandClassExtractor(geojson_file)
    
    # Extract land classes
    extractor.extract_land_classes()
    
    # Generate comprehensive report
    extractor.generate_summary_report()
    
    # Create visualizations
    extractor.create_color_palette_visualization()
    extractor.create_color_swatch_image()
    
    # Export color schemes
    extractor.export_color_schemes()
    
    print(f"\n" + "="*80)
    print("PROCESSING COMPLETE!")
    print("="*80)
    print("Output files created:")
    print("- land_classes_visualization.png (frequency chart)")
    print("- land_class_color_swatches.png (color reference)")
    print("- land_class_colors.json (web mapping format)")
    print("- land_class_colors.csv (spreadsheet format)")
    print("- land_class_arcgis_symbology.json (ArcGIS format)")

if __name__ == "__main__":
    main()
