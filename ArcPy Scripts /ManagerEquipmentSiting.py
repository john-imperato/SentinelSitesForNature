import arcpy
import os
import zipfile
import tempfile
import shutil
import datetime

def import_spatial_data_to_gdb():
    """
    Import all spatial data files from reserve folders into a geodatabase
    """
    
    # Set up paths
    source_folder = r"C:\SSN\GDB_setup\Biodiversity Monitoring Equipment Locations"
    target_gdb = r"C:\SSN\GDB_setup\EquipmentSiting.gdb"
    
    # Verify paths exist
    if not os.path.exists(source_folder):
        print(f"Error: Source folder does not exist: {source_folder}")
        return
    
    if not os.path.exists(target_gdb):
        print(f"Error: Target geodatabase does not exist: {target_gdb}")
        return
    
    # Set workspace
    arcpy.env.workspace = target_gdb
    arcpy.env.overwriteOutput = True
    
    # Create temporary directory for file processing
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Get all subdirectories (reserve folders)
        reserve_folders = [f for f in os.listdir(source_folder) 
                          if os.path.isdir(os.path.join(source_folder, f))]
        
        if not reserve_folders:
            print("No reserve folders found in source directory")
            return
        
        print(f"Found {len(reserve_folders)} reserve folders")
        
        successful_imports = 0
        failed_imports = []
        
        # Process each reserve folder
        for reserve_folder in reserve_folders:
            reserve_path = os.path.join(source_folder, reserve_folder)
            print(f"\nProcessing reserve: {reserve_folder}")
            
            # Find spatial data files in the reserve folder
            spatial_extensions = ['.kmz', '.kml', '.shp', '.geojson', '.json', '.gpx', '.gdb']
            spatial_files = []
            
            for file in os.listdir(reserve_path):
                file_lower = file.lower()
                for ext in spatial_extensions:
                    if file_lower.endswith(ext):
                        spatial_files.append(file)
                        break
            
            if not spatial_files:
                print(f"  No spatial data files found in {reserve_folder}")
                failed_imports.append(f"{reserve_folder} - No spatial data files found")
                continue
            
            print(f"  Found spatial files: {spatial_files}")
            
            # Process each spatial file in the reserve folder
            for spatial_file in spatial_files:
                spatial_path = os.path.join(reserve_path, spatial_file)
                
                try:
                    # Process different file types
                    success = process_spatial_file(spatial_path, reserve_folder, 
                                                 spatial_file, target_gdb, temp_dir)
                    
                    if success:
                        successful_imports += 1
                        print(f"  Successfully imported: {spatial_file}")
                    else:
                        failed_imports.append(f"{reserve_folder}/{spatial_file} - Conversion failed")
                        
                except Exception as e:
                    print(f"  Error processing {spatial_file}: {str(e)}")
                    failed_imports.append(f"{reserve_folder}/{spatial_file} - {str(e)}")
        
        # Print summary
        print(f"\n{'='*50}")
        print(f"IMPORT SUMMARY")
        print(f"{'='*50}")
        print(f"Successful imports: {successful_imports}")
        print(f"Failed imports: {len(failed_imports)}")
        
        if failed_imports:
            print(f"\nFailed imports:")
            for failure in failed_imports:
                print(f"  - {failure}")
    
    finally:
        # Clean up temporary directory
        try:
            shutil.rmtree(temp_dir)
        except:
            pass

def process_spatial_file(file_path, reserve_name, original_filename, target_gdb, temp_dir):
    """
    Process different types of spatial files and convert to feature class
    """
    file_ext = os.path.splitext(original_filename)[1].lower()
    
    try:
        print(f"    Processing {file_ext.upper()} file: {original_filename}")
        
        if file_ext == '.kmz':
            # Extract KMZ to KML first
            kml_path = extract_kmz_to_kml(file_path, temp_dir)
            if kml_path:
                return convert_kml_to_fc(kml_path, reserve_name, original_filename, target_gdb, temp_dir)
            else:
                return False
                
        elif file_ext == '.kml':
            # Process KML directly
            return convert_kml_to_fc(file_path, reserve_name, original_filename, target_gdb, temp_dir)
            
        elif file_ext == '.shp':
            # Process shapefile
            return convert_shapefile_to_fc(file_path, reserve_name, original_filename, target_gdb)
            
        elif file_ext in ['.geojson', '.json']:
            # Process GeoJSON
            return convert_geojson_to_fc(file_path, reserve_name, original_filename, target_gdb)
            
        elif file_ext == '.gpx':
            # Process GPX
            return convert_gpx_to_fc(file_path, reserve_name, original_filename, target_gdb, temp_dir)
            
        elif file_ext == '.gdb':
            # Process file geodatabase
            return convert_gdb_to_fc(file_path, reserve_name, original_filename, target_gdb)
            
        else:
            print(f"    Unsupported file type: {file_ext}")
            return False
            
    except Exception as e:
        print(f"    Error processing {original_filename}: {str(e)}")
        return False

def extract_kmz_to_kml(kmz_path, temp_dir):
    """
    Extract KML file from KMZ archive
    """
    try:
        with zipfile.ZipFile(kmz_path, 'r') as kmz:
            # Find KML file in the archive
            kml_files = [f for f in kmz.namelist() if f.lower().endswith('.kml')]
            
            if not kml_files:
                print(f"    No KML file found in {os.path.basename(kmz_path)}")
                return None
            
            # Extract the first KML file found
            kml_filename = kml_files[0]
            kml_path = os.path.join(temp_dir, f"temp_{os.path.basename(kmz_path)}.kml")
            
            with kmz.open(kml_filename) as kml_file:
                with open(kml_path, 'wb') as output_file:
                    output_file.write(kml_file.read())
            
            return kml_path
            
    except Exception as e:
        print(f"    Error extracting KMZ {os.path.basename(kmz_path)}: {str(e)}")
        return None

def convert_kml_to_fc(kml_path, reserve_name, original_filename, target_gdb, temp_dir):
    """
    Convert KML to feature class in geodatabase, filtering for points only
    """
    try:
        # Create output feature class name
        fc_name = clean_feature_class_name(reserve_name)
        output_fc = os.path.join(target_gdb, fc_name)
        
        # Create temporary output directory for KML conversion
        kml_output_dir = os.path.join(temp_dir, f"kml_output_{reserve_name}_{os.getpid()}")
        if not os.path.exists(kml_output_dir):
            os.makedirs(kml_output_dir)
        
        print(f"    Converting KML: {os.path.basename(kml_path)}")
        
        # Convert KML to layer
        arcpy.conversion.KMLToLayer(kml_path, kml_output_dir)
        
        # Look for created geodatabase
        created_files = os.listdir(kml_output_dir)
        gdb_files = [f for f in created_files if f.endswith('.gdb')]
        
        if gdb_files:
            temp_gdb = os.path.join(kml_output_dir, gdb_files[0])
            print(f"    DEBUG - Examining GDB: {temp_gdb}")
            
            # Check for feature classes and datasets
            original_workspace = arcpy.env.workspace
            arcpy.env.workspace = temp_gdb
            
            feature_classes = arcpy.ListFeatureClasses()
            datasets = arcpy.ListDatasets()
            print(f"    DEBUG - Root feature classes: {feature_classes}")
            print(f"    DEBUG - Datasets: {datasets}")
            
            # Look for point feature classes at root level
            if feature_classes:
                point_fcs = get_point_feature_classes(feature_classes, temp_gdb)
                if point_fcs:
                    return copy_point_features(point_fcs[0], output_fc, original_workspace, reserve_name, original_filename)
            
            # Look for point feature classes in datasets
            if datasets:
                for dataset in datasets:
                    dataset_path = os.path.join(temp_gdb, dataset)
                    print(f"    DEBUG - Checking dataset: {dataset}")
                    arcpy.env.workspace = dataset_path
                    dataset_fcs = arcpy.ListFeatureClasses()
                    print(f"    DEBUG - Feature classes in {dataset}: {dataset_fcs}")
                    
                    if dataset_fcs:
                        point_fcs = get_point_feature_classes(dataset_fcs, dataset_path)
                        if point_fcs:
                            source_fc = os.path.join(temp_gdb, dataset, point_fcs[0])
                            return copy_point_features(source_fc, output_fc, original_workspace, reserve_name, original_filename)
            
            arcpy.env.workspace = original_workspace
        else:
            print(f"    DEBUG - No GDB files created, checking for other outputs...")
            lyr_files = [f for f in created_files if f.endswith('.lyr') or f.endswith('.lyrx')]
            print(f"    DEBUG - Layer files: {lyr_files}")
            
            # Try alternative method: direct coordinate extraction from KML
            print(f"    Trying direct KML coordinate extraction...")
            return extract_coordinates_from_kml(kml_path, reserve_name, original_filename, target_gdb)
        
        print(f"    No point features found in KML: {original_filename}")
        return False
            
    except Exception as e:
        print(f"    Error converting KML to feature class: {str(e)}")
        try:
            arcpy.env.workspace = target_gdb
        except:
            pass
        return False

def convert_shapefile_to_fc(shp_path, reserve_name, original_filename, target_gdb):
    """
    Convert shapefile to feature class, filtering for points only
    """
    try:
        # Check geometry type
        desc = arcpy.Describe(shp_path)
        if desc.shapeType != "Point":
            print(f"    Skipping {desc.shapeType} shapefile: {original_filename}")
            return False
        
        # Create output feature class name
        fc_name = clean_feature_class_name(reserve_name)
        output_fc = os.path.join(target_gdb, fc_name)
        
        # Copy features
        arcpy.management.CopyFeatures(shp_path, output_fc)
        
        # Add metadata fields
        add_metadata_fields(output_fc, reserve_name, original_filename)
        
        return True
        
    except Exception as e:
        print(f"    Error converting shapefile: {str(e)}")
        return False

def convert_geojson_to_fc(geojson_path, reserve_name, original_filename, target_gdb):
    """
    Convert GeoJSON to feature class, filtering for points only
    """
    try:
        # Create output feature class name
        fc_name = clean_feature_class_name(reserve_name)
        output_fc = os.path.join(target_gdb, fc_name)
        
        # Convert GeoJSON to feature class
        arcpy.conversion.JSONToFeatures(geojson_path, output_fc)
        
        # Check if it contains points
        desc = arcpy.Describe(output_fc)
        if desc.shapeType != "Point":
            print(f"    Skipping {desc.shapeType} GeoJSON: {original_filename}")
            arcpy.management.Delete(output_fc)
            return False
        
        # Add metadata fields
        add_metadata_fields(output_fc, reserve_name, original_filename)
        
        return True
        
    except Exception as e:
        print(f"    Error converting GeoJSON: {str(e)}")
        return False

def convert_gpx_to_fc(gpx_path, reserve_name, original_filename, target_gdb, temp_dir):
    """
    Convert GPX to feature class, filtering for points only
    """
    try:
        # Create temporary output directory
        gpx_output_dir = os.path.join(temp_dir, f"gpx_output_{reserve_name}")
        if not os.path.exists(gpx_output_dir):
            os.makedirs(gpx_output_dir)
        
        # Convert GPX to features
        arcpy.conversion.GPXtoFeatures(gpx_path, gpx_output_dir)
        
        # Look for point feature classes
        original_workspace = arcpy.env.workspace
        arcpy.env.workspace = gpx_output_dir
        feature_classes = arcpy.ListFeatureClasses()
        
        point_fcs = get_point_feature_classes(feature_classes, gpx_output_dir)
        
        if point_fcs:
            # Create output feature class name
            fc_name = clean_feature_class_name(reserve_name)
            output_fc = os.path.join(target_gdb, fc_name)
            
            # Copy the first point feature class
            source_fc = os.path.join(gpx_output_dir, point_fcs[0])
            return copy_point_features(source_fc, output_fc, original_workspace, reserve_name, original_filename)
        else:
            print(f"    No point features found in GPX: {original_filename}")
            arcpy.env.workspace = original_workspace
            return False
        
    except Exception as e:
        print(f"    Error converting GPX: {str(e)}")
        arcpy.env.workspace = target_gdb
        return False

def convert_gdb_to_fc(gdb_path, reserve_name, original_filename, target_gdb):
    """
    Convert features from file geodatabase, filtering for points only
    """
    try:
        # Set workspace to source geodatabase
        original_workspace = arcpy.env.workspace
        arcpy.env.workspace = gdb_path
        
        # Get all feature classes
        feature_classes = arcpy.ListFeatureClasses()
        point_fcs = get_point_feature_classes(feature_classes, gdb_path)
        
        if point_fcs:
            # Create output feature class name
            fc_name = clean_feature_class_name(reserve_name)
            output_fc = os.path.join(target_gdb, fc_name)
            
            # Copy the first point feature class
            source_fc = os.path.join(gdb_path, point_fcs[0])
            return copy_point_features(source_fc, output_fc, original_workspace, reserve_name, original_filename)
        else:
            print(f"    No point features found in geodatabase: {original_filename}")
            arcpy.env.workspace = original_workspace
            return False
        
    except Exception as e:
        print(f"    Error converting geodatabase: {str(e)}")
        arcpy.env.workspace = target_gdb
        return False

def get_point_feature_classes(feature_classes, workspace):
    """
    Filter feature classes to return only point geometry
    """
    point_fcs = []
    for fc in feature_classes:
        try:
            desc = arcpy.Describe(os.path.join(workspace, fc))
            if desc.shapeType == "Point":
                point_fcs.append(fc)
                print(f"    Found point feature class: {fc}")
            else:
                print(f"    Skipping {desc.shapeType} feature class: {fc}")
        except:
            print(f"    Could not describe feature class: {fc}")
    return point_fcs

def copy_point_features(source_fc, output_fc, original_workspace, reserve_name, original_filename):
    """
    Copy point features and add metadata
    """
    try:
        arcpy.env.workspace = original_workspace
        arcpy.management.CopyFeatures(source_fc, output_fc)
        add_metadata_fields(output_fc, reserve_name, original_filename)
        return True
    except Exception as e:
        print(f"    Error copying features: {str(e)}")
        arcpy.env.workspace = original_workspace
        return False

def extract_coordinates_from_kml(kml_path, reserve_name, original_filename, target_gdb):
    """
    Direct extraction of point coordinates from KML when KMLToLayer fails
    """
    try:
        import xml.etree.ElementTree as ET
        
        # Parse the KML file
        tree = ET.parse(kml_path)
        root = tree.getroot()
        
        # KML namespace
        ns = {'kml': 'http://www.opengis.net/kml/2.2'}
        
        # Find all Point elements
        points = []
        for placemark in root.findall('.//kml:Placemark', ns):
            point_elem = placemark.find('.//kml:Point', ns)
            if point_elem is not None:
                coords_elem = point_elem.find('kml:coordinates', ns)
                if coords_elem is not None:
                    # Get placemark name
                    name_elem = placemark.find('kml:name', ns)
                    name = name_elem.text if name_elem is not None else "Unnamed"
                    
                    # Parse coordinates (lon,lat,alt format)
                    coords_text = coords_elem.text.strip()
                    if coords_text:
                        try:
                            parts = coords_text.split(',')
                            if len(parts) >= 2:
                                lon = float(parts[0])
                                lat = float(parts[1])
                                alt = float(parts[2]) if len(parts) > 2 else 0
                                points.append({
                                    'name': name,
                                    'lon': lon,
                                    'lat': lat,
                                    'alt': alt
                                })
                        except ValueError:
                            continue
        
        if points:
            print(f"    Found {len(points)} points via direct extraction")
            
            # Create feature class
            fc_name = clean_feature_class_name(reserve_name)
            output_fc = os.path.join(target_gdb, fc_name)
            
            # Create point feature class
            spatial_ref = arcpy.SpatialReference(4326)  # WGS84
            arcpy.management.CreateFeatureclass(
                target_gdb, 
                fc_name, 
                "POINT", 
                spatial_reference=spatial_ref
            )
            
            # Add name field
            arcpy.management.AddField(output_fc, "Name", "TEXT", field_length=100)
            
            # Insert points
            with arcpy.da.InsertCursor(output_fc, ["SHAPE@XY", "Name"]) as cursor:
                for point in points:
                    cursor.insertRow([(point['lon'], point['lat']), point['name']])
            
            # Add metadata fields
            add_metadata_fields(output_fc, reserve_name, original_filename)
            
            return True
        else:
            print(f"    No valid point coordinates found in KML")
            return False
            
    except Exception as e:
        print(f"    Direct KML extraction failed: {str(e)}")
        return False

def clean_feature_class_name(reserve_name):
    """
    Create a clean feature class name from reserve name
    """
    fc_name = reserve_name
    
    # Clean feature class name (remove special characters)
    fc_name = "".join(c for c in fc_name if c.isalnum() or c == "_")
    
    # Ensure it starts with a letter
    if fc_name and fc_name[0].isdigit():
        fc_name = f"FC_{fc_name}"
    
    # Truncate if too long (geodatabase limit is 64 characters)
    if len(fc_name) > 60:
        fc_name = fc_name[:60]
    
    return fc_name

def add_metadata_fields(feature_class, reserve_name, source_file):
    """
    Add metadata fields to track source information
    """
    try:
        # Add fields if they don't exist
        field_names = [f.name for f in arcpy.ListFields(feature_class)]
        
        if "Reserve_Name" not in field_names:
            arcpy.management.AddField(feature_class, "Reserve_Name", "TEXT", 
                                    field_length=50)
        
        if "Source_File" not in field_names:
            arcpy.management.AddField(feature_class, "Source_File", "TEXT", 
                                    field_length=100)
        
        if "Import_Date" not in field_names:
            arcpy.management.AddField(feature_class, "Import_Date", "DATE")
        
        # Populate metadata fields
        with arcpy.da.UpdateCursor(feature_class, 
                                 ["Reserve_Name", "Source_File", "Import_Date"]) as cursor:
            for row in cursor:
                row[0] = reserve_name
                row[1] = source_file
                row[2] = datetime.datetime.now()
                cursor.updateRow(row)
                
    except Exception as e:
        print(f"    Warning: Could not add metadata fields: {str(e)}")

if __name__ == "__main__":
    print("Starting spatial data import process...")
    import_spatial_data_to_gdb()
    print("Process completed!")