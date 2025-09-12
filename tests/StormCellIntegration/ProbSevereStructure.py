import json

def print_full_probsevere_structure(probsevere_path):
    """
    Print the complete structure of ProbSevere JSON data for debugging.
    """
    try:
        # Load ProbSevere data using standard json module
        with open(probsevere_path, 'r') as f:
            probsevere_data = json.load(f)
    except Exception as e:
        print(f"Failed to load ProbSevere data from {probsevere_path}: {e}")
        return
    
    print("\n" + "="*80)
    print("FULL PROBSEVERE JSON STRUCTURE ANALYSIS")
    print("="*80)
    
    # Print top-level keys
    print(f"Top-level keys: {list(probsevere_data.keys())}")
    print()
    
    # Print metadata
    print("METADATA:")
    for key in ['source', 'product', 'validTime', 'productionTime', 'machine', 'type']:
        if key in probsevere_data:
            print(f"  {key}: {probsevere_data[key]}")
    print()
    
    # Print features overview
    if 'features' in probsevere_data:
        features = probsevere_data['features']
        print(f"Number of features: {len(features)}")
        
        if features:
            # Print first 3 features in detail
            for i, feature in enumerate(features[:3]):
                print(f"\n--- Feature {i} ---")
                print(f"  Keys: {list(feature.keys())}")
                
                # Print geometry
                if 'geometry' in feature:
                    print(f"  Geometry type: {feature['geometry'].get('type', 'N/A')}")
                    if 'coordinates' in feature['geometry']:
                        coords = feature['geometry']['coordinates']
                        print(f"  Coordinates: {type(coords)}, length: {len(coords) if hasattr(coords, '__len__') else 'N/A'}")
                
                # Print models
                if 'models' in feature:
                    print(f"  Models keys: {list(feature['models'].keys())}")
                    for model_key, model_value in feature['models'].items():
                        print(f"    {model_key}: {type(model_value)}")
                
                # Print properties in detail
                if 'properties' in feature:
                    props = feature['properties']
                    print(f"  Properties keys: {list(props.keys())}")
                    print(f"  Total properties: {len(props)}")
                    
                    # Print all property key-value pairs
                    print("\n  Property values:")
                    for prop_key, prop_value in props.items():
                        print(f"    {prop_key}: {prop_value} ({type(prop_value)})")
                
                print("-" * 40)
            
            # Print summary of all property keys across features
            print(f"\nSUMMARY OF ALL PROPERTY KEYS ACROSS {len(features)} FEATURES:")
            all_property_keys = set()
            for feature in features:
                if 'properties' in feature:
                    all_property_keys.update(feature['properties'].keys())
            
            print(f"Unique property keys: {sorted(all_property_keys)}")
            
    else:
        print("No 'features' key found in ProbSevere data")
    
    print("="*80)

# Usage example
if __name__ == "__main__":
    probsevere_path = "C:/input_data/mrms_probsevere/MRMS_PROBSEVERE_20250901_202040.json"
    print_full_probsevere_structure(probsevere_path)