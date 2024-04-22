"""
https://airflow.apache.org/docs/apache-airflow/stable/howto/dynamic-dag-generation.html
"""

import json
import pathlib

from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dag_parsing_context import get_parsing_context

import include.global_variables as gv

current_dag_id = get_parsing_context().dag_id


def create_dag(
    dag_id,
    schedule,
    start_date,
    default_args,
    tags: list[str],
):
    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date,
        default_args=default_args,
        catchup=False,
        tags=tags,
    )
    def dynamic_generated_dag():

        @task()
        def print_message():
            print(f"Hello World: {gv.TEST}")

        print_message()

    generated_dag = dynamic_generated_dag()

    return generated_dag


for config in json.loads(
    (pathlib.Path(__file__).parent / "conf/hello_dag.json").read_text(encoding="utf-8")
):
    dag_id = f"{Variable.get('gd_prefix')}_{config['name']}"

    # Optimizing DAG parsing delays during execution
    #   docs: https://airflow.apache.org/docs/apache-airflow/stable/
    #       howto/dynamic-dag-generation.html#optimizing-dag-parsing-delays-during-execution
    if current_dag_id is not None and current_dag_id != dag_id:
        continue  # skip generation of non-selected DAG

    @dag(
        dag_id=dag_id,
        schedule=None,
        start_date=datetime(2024, 4, 1),
        catchup=False,
        tags=["generator"],
    )
    def dynamic_generated_dag():
        @task
        def print_message(message):
            print(f"Hello World: {gv.TEST}")
            print(message)

        print_message(config["message"])

    dynamic_generated_dag()


def generate_with_connection():
    from airflow import settings
    from airflow.models import Connection

    session = settings.Session()

    # adjust the filter criteria to filter which of your connections to use
    # to generated your DAGs
    conns = (
        session.query(Connection.conn_id)
        .filter(Connection.conn_id.ilike("%MY_DATABASE_CONN%"))
        .all()
    )
    for conn in conns:
        dag_id = f"connection_hello_world_{conn[0]}"
        default_args = {"owner": "airflow", "start_date": datetime(2013, 7, 1)}
        schedule = "@daily"
        dag_number = conn
        globals()[dag_id] = create_dag(
            dag_id,
            schedule,
            dag_number,
            default_args,
        )
