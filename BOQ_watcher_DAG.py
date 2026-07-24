import glob
import json
import os
import re

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task, Variable

INPUT_FOLDER = Variable.get("BOQ_INPUT_FOLDER")
MANIFEST_PATH = os.path.expanduser("~/airflow/data/boq_watcher_manifest.json")

def _load_manifest():
    """ Returns the set of filenames we've already triggered a run for. """
    if not os.path.exists(MANIFEST_PATH):
        return set()
    with open(MANIFEST_PATH) as f:
        return set(json.load(f))

def _save_manifest(seen):
    os.makedirs(os.path.dirname(MANIFEST_PATH), exist_ok=True)
    with open(MANIFEST_PATH, "w") as f:
        json.dump(sorted(seen), f)

@dag(
    dag_id="BOQ_Watcher_DAG",
    start_date=pendulum.datetime(2026, 7, 24),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=['BOQ'],
)
def boq_watcher():

    @task
    def find_new_files():
        """ Scan Input/ folder for .dwg files not already in the manifest, and return one conf dict per new file for the trigger step below. """
        seen = _load_manifest()

        all_dwg_files = glob.glob(os.path.join(INPUT_FOLDER, "*.dwg"))
        new_filenames = [os.path.basename(p) for p in all_dwg_files if os.path.basename(p) not in seen]

        if not new_filenames:
            print("No new files found.")
            return []

        confs = []
        for filename in new_filenames:
            project_name = re.sub(r"\s*-\s*BOQ\.dwg$", "", filename)
            confs.append({"filename": filename, "detected_project_name": project_name})
            print(f"New file detected: {filename} -> project_name = {project_name}")

        return confs

    new_file_confs = find_new_files()

    trigger_main_dag = TriggerDagRunOperator.partial(
        task_id="trigger_main_dag",
        trigger_dag_id="BOQ_DAG",
    ).expand(conf=new_file_confs)

    @task
    def mark_seen(conf, _trigger_result):
        """ Only mark the project as seen if the trigger_main_dag actually succeeded """
        seen = _load_manifest()
        seen.add(conf["filename"])
        _save_manifest(seen)
        print(f"Marked as seen: {conf['filename']}")

    mark_seen.expand(conf=new_file_confs, _trigger_result=trigger_main_dag.output)

BOQ_watcher_DAG = boq_watcher()