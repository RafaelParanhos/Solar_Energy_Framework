from DWG_to_DuckDB_ETL import *
import os
from datetime import datetime

def weekly_extraction_job():
    '''
    Extraction function for LV trenches from PV Riodades.
    '''
    dwg_folder = r"C:\Users\Usuario\Ecotechnee\Office - Documentos\Projetos\309 - Zagope - UVF Riodades\01 - Projetos\01 - Em Confecção\02 - Edição\01 - Layout\BT - 17.03.26"
    output_folder = os.path.join(dwg_folder, "output")

    os.makedirs(output_folder, exist_ok=True)

    timestamp = datetime.now().strftime("%d.%m.%Y_%H-%M-%S")
    output_timestamped_folder = os.path.join(output_folder, timestamp)

    os.makedirs(output_timestamped_folder, exist_ok=True)

    processed_count = 0
    error_count = 0

    print(f"{'='*180}")
    print(f"Starting DWG Extraction Job")
    print(f"Source folder: {dwg_folder}")
    print(f"Output folder: {output_timestamped_folder}")
    print(f"{'='*180}\n")

    dwg_files = [f for f in os.listdir(dwg_folder) if f.lower().endswith(".dwg")]

    if not dwg_files:
        print("No DWG Files Found")
        return

    print(f"Found {len(dwg_files)} DWG Files")

    convert_dwg_to_dxf(dwg_folder, output_folder)
    dxf_files = [f for f in os.listdir(output_folder) if f.lower().endswith(".dxf")]

    for dxf_file in dxf_files:
        try:
            # Full paths
            dxf_path = os.path.join(output_folder, dxf_file)
            xlsx_path = os.path.join(output_timestamped_folder, f"{dxf_file[:-4]}_{timestamp}.xlsx")

            # Step 1 - Error handling
            if not os.path.exists(dxf_path):
                raise Exception(f" DXF file {dxf_path} not created.")

            # Step 2 - Creating the DataFrame
            data = extract_data_from_dxf(dxf_path)

            # Step 3 - Saving the data to Excel
            save_to_excel(data, xlsx_path)

            # Step 4 - Loading the data into Duckdb
            load_to_duckdb(dxf_path, 'PV Riodades - Trenches')

            # Step 5 - Cleanup
            os.remove(dxf_path)

            processed_count += 1

        except Exception as e:
            print(f"Error processing {dxf_file}: {e}")
            error_count += 1

            try:
                if os.path.exists(dxf_path):
                    os.remove(dxf_path)

            except:
                pass

        except:
            pass

        print(f"{'=' * 60}")
        print(f"SUMMARY: {processed_count} files processed successfully, {error_count} errors")
        print(f"Excel files saved to: {output_timestamped_folder}")
        print(f"{'=' * 60}")

if __name__ == "__main__":
    weekly_extraction_job()

