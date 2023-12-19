#!/bin/python

import requests
import os
import argparse

AIRFLOW_SERVER = os.getenv("AIRFLOW_SERVER", "http://localhost:8080")
AIRFLOW_API_USERNAME = os.getenv("AIRFLOW_API_USERNAME", "airflow")
AIRFLOW_API_PASSWORD = os.getenv("AIRFLOW_API_PASSWORD", "airflow")
AIRFLOW_API = f"{AIRFLOW_SERVER}/rest_api/api"
AUTH = (AIRFLOW_API_USERNAME, AIRFLOW_API_PASSWORD)


def valid_dag_file(file_path):
    if not os.path.isfile(file_path):
        print("The specified file path does not exist.")
        return

    if not file_path.lower().endswith(".py"):
        print("The file must have a '.py' extension.")
        return
    return True


def deploy_dag(file_path, pause=True, force=False):
    if not valid_dag_file(file_path):
        return
    params = {"api": "deploy_dag", "pause": pause, "force": force}
    files = {"dag_file": open(file_path, "rb")}
    print(
        f"Deploying DAG with file_path={file_path} and pause={pause} and force={force}"
    )

    response = requests.get(AIRFLOW_API, auth=AUTH, params=params, files=files)
    print(response.text)
    return response


def initialize_parser():
    parser = argparse.ArgumentParser(description="Deploy DAG file to Airflow")
    parser.add_argument("api", help="api method")
    parser.add_argument(
        "--file_path", help="Path to the DAG file (optional)", default=None
    )
    parser.add_argument(
        "--pause", action="store_true", help="Pause the DAG (default: True)"
    )
    parser.add_argument(
        "--force", action="store_true", help="Force the deployment (default: False)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = initialize_parser()

    if args.api == "deploy_dag":
        deploy_dag(
            file_path=args.file_path,
            pause=args.pause,
            force=args.force,
        )
