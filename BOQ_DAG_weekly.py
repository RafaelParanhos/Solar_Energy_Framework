import pendulum
import pandas as pd
import duckdb
from datetime import datetime, timedelta
import os

from airflow.models import Param
from airflow.providers.standard.operators.hitl import HITLEntryOperator, HITLOperator
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk import BaseNotifier, Context, dag, task, get_current_context
from pip._internal.models import candidate
from pygments.styles import default


class LocalLogNotifier(BaseNotifier):
    """Simple notifier to demonstrate HITL notification without setup any connection."""

    template_fields = ("message",)

    def __init__(self, message: str) -> None:
        self.message = message

    def notify(self, context: Context) -> None:
        url = HITLOperator.generate_link_to_ui_from_context(
            context=context,
            base_url="http://localhost:28080",
        )
        self.log.info(self.message)
        self.log.info("Url to respond %s", url)

hitl_request_callback = LocalLogNotifier(
    message="""
[HITL]
Subject: {{ task.subject }}
Body: {{ task.body }}
Options: {{ task.options }}
Is Multiple Option: {{ task.multiple }}
Default Options: {{ task.defaults }}
Params: {{ task.params }}
"""
)
hitl_success_callback = LocalLogNotifier(
    message="{% set task_id = task.task_id -%}{{ ti.xcom_pull(task_ids=task_id) }}"
)
hitl_failure_callback = LocalLogNotifier(message="Request to response to '{{ task.subject }}' failed")

def get_db_connection(read_only=False):
    """ Auxiliary function for connecting to DB """
    conn_id = "duckdb_default"
    hook = BaseHook.get_connection(conn_id)

    base_path = hook.host
    db_name = "pv_projects.duckdb"
    db_path = os.path.join(base_path, db_name)
    return duckdb.connect(db_path, read_only=read_only)

def close_db_connection(conn):
    """ Auxiliary function for closing DB """
    if conn:
        conn.close()



@dag(
    dag_id='BOQ_DAG_weekly',
    start_date=pendulum.datetime(2026, 7, 21),
    schedule="@weekly",
    catchup=False,
    tags=['BOQ'],
    max_active_runs=1,
)
def bill_of_quantities():
    """
    DAG for extracting BOQ weekly data from .dwg file
    :return:
    """
    wait_for_input = HITLEntryOperator(
        task_id="wait_for_input",
        subject="Please provide name of the project information: ",
        params={"information": Param("", type="string")},
        notifiers=[hitl_request_callback],
        on_success_callback=hitl_success_callback,
        on_failure_callback=hitl_failure_callback,
    )

    @task
    def process_user_input(hitl_output=None):
        """ Receives the user input from the HITL Operator. """
        user_input = hitl_output["params_input"]["information"]

        print(f"User provided: {user_input}")

        return user_input


    WORKDIR = os.path.expanduser("~/airflow/data/boq_files")

    @task
    def convert_dwg_to_dxf(project_name):
        import re
        import glob
        import shutil
        import subprocess

        context = get_current_context()
        run_id_safe = re.sub(r"[^A-Za-z0-9_.-]", "_", context["run_id"])

        dwg_folder = "/mnt/c/Users/Usuario/Desktop/Pessoal/Data Engineering/Airflow/BOQ/Input"
        temp_folder = os.path.join(dwg_folder, "temp")
        output_folder = dwg_folder

        dwg_filename = f"{project_name} - BOQ.dwg"
        dwg_file = os.path.join(output_folder, dwg_filename)

        if not os.path.exists(dwg_file):
            raise FileNotFoundError(f'DWG file not found: {dwg_file}')

        work_dir = os.path.join(WORKDIR, project_name, run_id_safe)

        in_dir= os.path.join(work_dir, "in")
        out_dir = os.path.join(work_dir, "out")
        os.makedirs(in_dir, exist_ok=True)
        os.makedirs(out_dir, exist_ok=True)

        staged_input = os.path.join(in_dir, dwg_filename)
        shutil.copy2(dwg_file, staged_input)

        oda_exe = os.environ.get("ODA_FILE_CONVERTER_PATH")
        if not oda_exe:
            candidates = (
                    glob.glob("/usr/bin/ODAFileConverter")
                    + glob.glob("/opt/ODA/ODAFileConverter*/ODAFileConverter")
            )
            if not candidates:
                raise FileNotFoundError(
                    "ODAFileConverter not found. Install the Linux .deb from "
                    "https://www.opendesign.com/guestfiles/oda_file_converter, "
                    "or set ODA_FILE_CONVERTER_PATH."
                )
            oda_exe = candidates[0]

        if shutil.which("xvfb-run") is None:
            raise EnvironmentError(
                "xvfb-run not found. Install it with `sudo apt-get install -y xvfb` "
                "so ODAFileConverter can run without a real display."
            )

        cmd = [
            "xvfb-run", "-a", "--server-args=-screen 0 1024x768x24",
            oda_exe,
            in_dir, out_dir,
            "ACAD2018", "DXF", "0", "1",
            dwg_filename,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if result.returncode != 0:
            raise RuntimeError(
                f"ODA File Converter failed (exit {result.returncode}).\n"
                f"stdout: {result.stdout}\nstderr: {result.stderr}"
            )

        converted = os.path.join(out_dir, f"{project_name} - BOQ.dxf")
        if not os.path.exists(converted):
            raise RuntimeError(
                f"Conversion produced no output file.\nstdout: {result.stdout}\nstderr: {result.stderr}"
            )

        dxf_file = os.path.join(work_dir, f"{project_name} - BOQ.dxf")
        shutil.move(converted, dxf_file)
        shutil.rmtree(in_dir, ignore_errors=True)
        shutil.rmtree(out_dir, ignore_errors=True)

        print("Data Loaded")
        return dxf_file

    @task
    def extract_data_from_dxf(dxf_path):
        import math
        import ezdxf
        from ezdxf.math import Vec3

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
                entity_data['length'] = round(math.sqrt((entity_data['end_x'] - entity_data['start_x']) ** 2 + (
                            entity_data['end_y'] - entity_data['start_y']) ** 2), 4)
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

                    if i == len(points) - 1:
                        if not entity.closed:
                            break
                        x2, y2, sw2, ew2, bulge2 = points[0]

                    else:
                        x2, y2, sw2, ew2, bulge2 = points[i + 1]

                    start = Vec3(x1, y1)
                    end = Vec3(x2, y2)

                    if bulge1 != 0:
                        chord_length = (end - start).magnitude
                        angle = 4 * math.atan(abs(bulge1))
                        radius = abs(chord_length / (2 * math.sin(angle / 2)))
                        arc_length = radius * angle
                        length += arc_length
                    else:
                        length += (end - start).magnitude

                entity_data['length'] = length
            elif entity.dxftype() == "INSERT":
                entity_data['name'] = entity.dxf.name
                entity_data['X'] = entity.dxf.insert.x
                entity_data['Y'] = entity.dxf.insert.y
                entity_data['rotation'] = entity.dxf.rotation
                entity_data['color'] = entity.dxf.color
                for attrib in entity.attribs:
                    entity_data[f'{attrib.dxf.tag}'] = attrib.dxf.text

            data.append(entity_data)

        os.remove(dxf_path)

        return data

    @task
    def load_to_duckdb(data, project_name, rewrite=True):
        """
        Loads PV project into main database
        If there is already data from the pv_project, it deletes old data and adds the new as there is no need to maintain old data.
        """
        conn = get_db_connection()
        try:
            df = pd.DataFrame(data)

            df['project_name'] = project_name
            df['ingestion_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            all_columns = [
                'project_name', 'type', 'layer', 'handle', 'name',
                'start_x', 'start_y', 'end_x', 'end_y', 'length',
                'center_x', 'center_y', 'radius', 'major_radius',
                'X', 'Y', 'rotation',
                'PRK', 'UFV', 'PST', 'INV', 'EFX', 'STR', 'GRP',
                'QBT', 'DSJ', 'PVM',
                'ingestion_timestamp',
                'color'
            ]

            for col in all_columns:
                if col not in df.columns:
                    df[col] = None

            if 'ITS' in df.columns and 'PST' in df.columns:
                # Fill PST with ITS values where PST is null/empty
                df['PST'] = df['PST'].fillna(df['ITS'])

            df = df[all_columns]

            conn.register("df_temp", df)
            conn.execute("""
                        CREATE TABLE IF NOT EXISTS cad_entities (
                            project_name VARCHAR,
                            type VARCHAR,
                            layer VARCHAR,
                            handle VARCHAR,
                            name VARCHAR,
                            start_x FLOAT,
                            start_y FLOAT,
                            end_x FLOAT,
                            end_y FLOAT,
                            length FLOAT,
                            center_x FLOAT,
                            center_y FLOAT,
                            radius FLOAT,
                            major_radius FLOAT,
                            X FLOAT,
                            Y FLOAT,
                            rotation FLOAT,
                            PRK VARCHAR,
                            UFV VARCHAR,
                            PST VARCHAR,
                            INV VARCHAR,
                            EFX VARCHAR,
                            STR VARCHAR,
                            GRP VARCHAR,
                            QBT VARCHAR,
                            DSJ VARCHAR,
                            PVM VARCHAR,
                            ingestion_timestamp TIMESTAMP,
                            color VARCHAR
                        )
                         """)

            conn.execute("BEGIN TRANSACTION")

            try:
                existing = conn.execute("SELECT COUNT(DISTINCT project_name) FROM cad_entities WHERE project_name = ?",
                                        [project_name]).fetchone()[0]
                if existing > 0 and rewrite:
                    print(f"Project {project_name} already exists")
                    conn.execute("DELETE FROM cad_entities WHERE project_name = ?", [project_name])
                    print(f"Removed existing data for project: {project_name}")
                elif existing == 0 and not rewrite:
                    print(f"Project {project_name} already exists")
                    print(f"Inserting more data for project: {project_name}")

                conn.execute("INSERT INTO cad_entities SELECT * FROM df_temp")

                conn.execute("COMMIT")
                print("Successfully loaded project")
                exit_code = 0

            except Exception as e:
                conn.execute("ROLLBACK")
                print(f"Failed to load project: {project_name}: {e}")
                raise

            print(f"Loaded {len(df)} entities for project: {project_name}")
            print("Don't forget to close connection.")
        finally:
            close_db_connection(conn)

        return exit_code

    @task
    def boq_transformations_structures(project_name):
        conn = get_db_connection(read_only=True)
        try:
            tracker_data = conn.sql(
                f"SELECT layer, X, Y, FROM cad_entities WHERE project_name='{project_name}' AND name ILIKE '%Tracker%'")
            structure_df = tracker_data.fetchdf()
            structure_df['Line'] = round(structure_df['X'],0)

            structure_df.sort_values(by=['Line','Y'], ascending=[True,False], inplace=True, ignore_index=True)

            # Different Lines
            condition1 = structure_df["Line"] != structure_df["Line"].shift()

            # Break in the Structure with no C type on the edges
            condition2 = (abs(structure_df["Y"] - structure_df["Y"].shift()) > 37.5)

            break_condition = condition1 | condition2
            structure_df['Group'] = break_condition.cumsum()

            number_of_structures = pd.DataFrame({'Number of Structures': max(structure_df['Group'])}, index=[0])
        finally:
            close_db_connection(conn)

        return number_of_structures.to_dict(orient='records')

    @task
    def boq_transformations_cables(project_name):
        conn = get_db_connection(read_only=True)
        try:
            cable_data = conn.sql(
                f"SELECT layer, start_x, start_y, end_x, end_y, length FROM cad_entities WHERE project_name='{project_name}' AND layer ILIKE '%vala%' OR layer ILIKE '%cabo%' OR layer ILIKE '%eletroduto%'")

            ac_cables = conn.sql("""
                SELECT layer, start_x, start_y, end_x, end_y, length FROM cable_data WHERE layer LIKE 'EMF_bt_vala_btca_%xC'
            """)

            dc_cables = conn.sql("""
                SELECT layer, start_x, start_y, end_x, end_y, length FROM cable_data WHERE layer = 'EMF_cabo_solar_negativo' OR layer = 'EMF_cabo_solar_positivo'
            """)

            dc_ducts = conn.sql("""
                SELECT layer, start_x, start_y, end_x, end_y, length FROM cable_data WHERE layer LIKE 'EMF_bt_eletroduto_%S'
            """)

            dc_trenches = conn.sql("""
                SELECT layer, start_x, start_y, end_x, end_y, length FROM cable_data WHERE layer LIKE 'EMF_vala_CC_%C'
            """)

            ac_cables_group = conn.sql("SELECT layer,SUM(length) as total_length FROM 'ac_cables' GROUP BY layer ORDER BY layer")
            dc_cables_group = conn.sql("SELECT layer,SUM(length) as total_length FROM 'dc_cables' GROUP BY layer ORDER BY layer")
            longest_DC_cables = conn.sql("SELECT * FROM dc_cables ORDER BY length DESC LIMIT 10")
            dc_ducts_group = conn.sql("SELECT layer,SUM(length) as total_length FROM 'dc_ducts' GROUP BY layer ORDER BY layer")
            dc_ducts_count = conn.sql("SELECT layer, COUNT(layer) as count FROM 'dc_ducts' GROUP BY layer ORDER BY count DESC")
            dc_trenches_group = conn.sql("SELECT layer,SUM(length) as total_length FROM 'dc_trenches' GROUP BY layer ORDER BY layer")


            processed_cable_data = {
                'AC Cables': ac_cables_group.fetchdf().to_dict(orient='records'),
                'DC Cables': dc_cables_group.fetchdf().to_dict(orient='records'),
                'Longest DC Cables': longest_DC_cables.fetchdf().to_dict(orient='records'),
                'DC Ducts': dc_ducts_group.fetchdf().to_dict(orient='records'),
                'Qtd. DC Ducts': dc_ducts_count.fetchdf().to_dict(orient='records'),
                'DC Trenches': dc_trenches_group.fetchdf().to_dict(orient='records'),
            }
        finally:
            close_db_connection(conn)

        return processed_cable_data


    @task
    def load_to_excel(number_of_structures, processed_cable_data, project_name):
        import shutil
        import time

        ac_cables_group = pd.DataFrame(processed_cable_data['AC Cables'])
        dc_cables_group = pd.DataFrame(processed_cable_data['DC Cables'])
        longest_DC_cables = pd.DataFrame(processed_cable_data['Longest DC Cables'])
        dc_ducts_group = pd.DataFrame(processed_cable_data['DC Ducts'])
        dc_ducts_count = pd.DataFrame(processed_cable_data['Qtd. DC Ducts'])
        dc_trenches_group = pd.DataFrame(processed_cable_data['DC Trenches'])

        date = datetime.now().strftime("%d.%m.%Y")
        filename = f"{project_name} - BOQ - {date}.xlsx"

        work_dir = os.path.join(WORKDIR, project_name)
        os.makedirs(work_dir, exist_ok=True)
        local_xlsx_path = os.path.join(work_dir, filename)

        with pd.ExcelWriter(local_xlsx_path) as writer:
            ac_cables_group.to_excel(writer, sheet_name='AC Trenches (m)', index=False)
            dc_cables_group.to_excel(writer, sheet_name='DC Cables (m)', index=False)
            longest_DC_cables.to_excel(writer, sheet_name='Longest DC Cables (m)', index=False)
            dc_ducts_group.to_excel(writer, sheet_name='DC Ducts (m)', index=False)
            dc_ducts_count.to_excel(writer, sheet_name='DC Ducts (un.)', index=False)
            dc_trenches_group.to_excel(writer, sheet_name='DC Trenches (un.)', index=False)
            pd.DataFrame(number_of_structures).to_excel(writer, sheet_name='Number of Structures', index=False)


        main_dest_folder = "/mnt/c/Users/Usuario/Desktop/Pessoal/Data Engineering/Airflow/BOQ/Output"
        dest_folder  = os.path.join(main_dest_folder, project_name)
        os.makedirs(dest_folder, exist_ok=True)
        dest_xlsx_path = os.path.join(dest_folder, filename)
        shutil.copy2(local_xlsx_path, dest_xlsx_path)

        time.sleep(3)
        local_size = os.path.getsize(local_xlsx_path)
        if not os.path.exists(dest_xlsx_path) or os.path.getsize(dest_xlsx_path) != local_size:
            raise RuntimeError(
                f"Report vanished or was altered after copying to '{dest_xlsx_path}' — same "
                f"silent-deletion behavior seen earlier with the DXF conversion output on this "
                f"Windows path. The report is safe at: {local_xlsx_path}"
            )

        print(f"Report saved to {dest_xlsx_path}")



    # Create task instances with their dependencies
    user_input = process_user_input(wait_for_input.output)
    dxf_file = convert_dwg_to_dxf(user_input)
    dwg_data = extract_data_from_dxf(dxf_file)
    data_loaded = load_to_duckdb(dwg_data, user_input)

    # Set the chain
    wait_for_input >> user_input >> dxf_file >> dwg_data >> data_loaded

    # Create downstream tasks
    structures = boq_transformations_structures(user_input)
    cables = boq_transformations_cables(user_input)
    final = load_to_excel(structures, cables, user_input)

    # Set dependencies
    data_loaded >> [structures, cables] >> final

BOQ_DAG_weekly = bill_of_quantities()


