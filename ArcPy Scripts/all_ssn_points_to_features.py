import arcpy, os
from arcpy import management as DM

arcpy.env.overwriteOutput = True

input_csv = r"\\Mac\Home\Desktop\SSN\all_ssn_coords.csv"

out_gdb = r"C:\SSN\GIS\SentinelSitesData.gdb"
out_fc_name = "SSN_All_Sites"
x_field = "lon"
y_field = "lat"  
in_sr = arcpy.SpatialReference(4326)  # WGS84
# -------------------

def ensure_gdb(path):
    folder, name = os.path.split(path)
    if folder and not os.path.exists(folder):
        os.makedirs(folder)
    if not os.path.exists(path):
        DM.CreateFileGDB(folder, name)

def main():
    if not arcpy.Exists(out_gdb):
        ensure_gdb(out_gdb)
    if not os.path.exists(input_csv):
        raise FileNotFoundError(f"CSV not found: {input_csv}")

    out_fc = os.path.join(out_gdb, out_fc_name)

    # Create points from CSV
    DM.XYTableToPoint(
        in_table=input_csv,
        out_feature_class=out_fc,
        x_field=x_field,
        y_field=y_field,
        coordinate_system=in_sr
    )

    # Optional: add X/Y fields in current CRS
    try:
        DM.AddGeometryAttributes(out_fc, "POINT_X_Y_Z_M")
    except Exception:
        pass

    print(f"✅ Created feature class: {out_fc}")

if __name__ == "__main__":
    main()


