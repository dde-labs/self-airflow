from datetime import datetime
from airflow.decorators import dag, task

from operators import HelloOperator
from helpers.utils import print_utils


@dag(
    dag_id="dag_hello_operator",
    schedule=None,
    start_date=datetime(2024, 4, 1),
    catchup=False,
    tags=["simple"],
)
def dynamic_generated_dag():
    HelloOperator(
        task_id="sample-task",
        name="foo_bar",
    )

    @task()
    def print_message():
        print_utils()


dynamic_generated_dag()
