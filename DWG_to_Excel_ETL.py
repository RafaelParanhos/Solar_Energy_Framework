import math
import pandas as pd
import subprocess
import ezdxf
from ezdxf.math import Vec3
import duckdb
import datetime

def convert_dwg_to_dxf(dwg_folder, output_folder):
    oda_path = r"C:\Program Files\ODA\ODAFileConverter 27.1.0\ODAFileConverter.exe"
    cmd = [oda_path, dwg_folder, output_folder, "ACAD2018", "DXF", "0", "1"]
    subprocess.run(cmd, capture_output = True)

def extract_data_from_dxf(dxf_path):
    data = []
    doc = ezdxf.readfile(dxf_path)
    msp = doc.modelspace()

    for entity in msp:
        entity_data = {
            'type': entity.dxftype(),
            'layer': entity.dxf.layer,
            'handle': entity.dxf.handle
        }
        if entity.dxftype() == "LINE":
            entity_data['start_x'] = entity.dxf.start.x
            entity_data['start_y'] = entity.dxf.start.y
            entity_data['end_x'] = entity.dxf.end.x
            entity_data['end_y'] = entity.dxf.end.y
            entity_data['length'] = round(math.sqrt((entity_data['end_x'] - entity_data['start_x'])**2 + (entity_data['end_y'] - entity_data['start_y'])**2),4)
        elif entity.dxftype() == "CIRCLE":
            entity_data['center_x'] = entity.dxf.center.x
            entity_data['center_y'] = entity.dxf.center.y
            entity_data['radius'] = entity.dxf.radius
        elif entity.dxftype() == "ELLIPSE":
            entity_data['center_x'] = entity.dxf.center.x
            entity_data['center_y'] = entity.dxf.center.y
            entity_data['major_radius'] = entity.dxf.major_axis.magnitude
        elif entity.dxftype() == "LWPOLYLINE":
            length = 0.0
            points = list(entity.get_points())
            entity_data['start_x'] = points[0][0]
            entity_data['start_y'] = points[0][1]
            entity_data['end_x'] = points[-1][0]
            entity_data['end_y'] = points[-1][1]

            for i in range(len(points)):
                x1, y1, sw1, ew1, bulge1 = points[i]

                if i == len(points)-1:
                    if not entity.closed:
                        break
                    x2, y2, sw2, ew2, bulge2 = points[0]

                else:
                    x2, y2, sw2, ew2, bulge2 = points[i+1]

                start = Vec3(x1, y1)
                end = Vec3(x2, y2)

                if bulge1 != 0:
                    chord_length = (end-start).magnitude
                    angle = 4 * math.atan(abs(bulge1))
                    radius = abs(chord_length / (2*math.sin(angle / 2)))
                    arc_length = radius * angle
                    length += arc_length
                else:
                    length += (end-start).magnitude

            entity_data['length'] = length
        elif entity.dxftype() == "INSERT":
            entity_data['name'] = entity.dxf.name
            entity_data['X'] = entity.dxf.insert.x
            entity_data['Y'] = entity.dxf.insert.y
            entity_data['rotation'] = entity.dxf.rotation
            for attrib in entity.attribs:
                entity_data[f'{attrib.dxf.tag}'] = attrib.dxf.text

        data.append(entity_data)

    return data


def save_to_excel(data, output_file):
    if not data:
        return


    data_df = pd.DataFrame(data)
    data_df.drop(columns=['type','handle'], inplace=True, errors='ignore')




    with pd.ExcelWriter(output_file) as writer:
        data_df.to_excel(writer, sheet_name="Trench Lengths")
        data_df.groupby(by="layer").sum().to_excel(writer, sheet_name="Total Lengths")


# Add this debug code to see if the DXF itself has negative coordinates
def debug_dxf_coordinates(dxf_path):
    doc = ezdxf.readfile(dxf_path)
    msp = doc.modelspace()

    for entity in msp:
        if entity.dxftype() == "CIRCLE":
            print(f"Circle at center: ({entity.dxf.center.x}, {entity.dxf.center.y})")
            print(f"Extrusion: {entity.dxf.extrusion}")
            print("---")

def load_to_duckdb(dxf_path, project_name):
    ''' Loads PV project into main database '''

    conn = duckdb.connect("pv_projects.duckdb")

    data = extract_data_from_dxf(dxf_path)
    df = pd.DataFrame(data)

    df['project_name'] = project_name
    df['ingestion_timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn.register("df_temp", df)
    conn.execute("CREATE TABLE IF NOT EXISTS cad_entities AS SELECT * FROM df_temp")

    print(f"Loaded {len(df)} entities for project: {project_name}")

    return conn