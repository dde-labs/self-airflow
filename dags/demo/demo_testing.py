import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.utils import timezone


def _hello(**context):
    logging.info(f"{context}")

    ds = context["ds"]
    data_interval_start = context["data_interval_start"]

    logging.info(f"ds: {ds}")
    logging.info(f"data_interval_start: {data_interval_start}")
    logging.info("Hello")


def _world():
    logging.info("World")


default_args = {
    "owner": "korawica",
    "start_date": timezone.datetime(2024, 2, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}
with DAG(
    "demo_testing_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    hello = PythonOperator(
        task_id="hello",
        python_callable=_hello,
    )

    world = PythonOperator(
        task_id="world",
        python_callable=_world,
    )

    empty_task_13 = EmptyOperator(task_id="date_in_range")
    empty_task_23 = EmptyOperator(task_id="date_outside_range")

    cond3 = BranchDateTimeOperator(
        task_id="datetime_branch",
        use_task_logical_date=True,
        follow_task_ids_if_true=["date_in_range"],
        follow_task_ids_if_false=["date_outside_range"],
        target_upper=pendulum.datetime(2024, 10, 10, 15, 0, 0),
        target_lower=pendulum.datetime(2024, 10, 10, 14, 0, 0),
    )

    hello >> world

    # Run empty_task_13 if cond3 executes between 2020-10-10 14:00:00
    # and 2020-10-10 15:00:00
    cond3 >> [empty_task_13, empty_task_23]

    world >> cond3
