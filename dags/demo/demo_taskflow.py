import logging
from datetime import datetime

import requests
from airflow.decorators import dag, task, setup, teardown
from airflow.operators.email import EmailOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.weekday import WeekDay


API: str = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids=bitcoin&vs_currencies=usd&include_market_cap=true"
    "&include_24hr_vol=true&include_24hr_change=true"
    "&include_last_updated_at=true"
)


@dag(
    schedule="@daily",
    start_date=datetime(2021, 12, 1),
    catchup=False,
)
def taskflow():
    @setup
    def my_setup_task():
        logging.info("Setting up resources!")
        return "Cluster 01"

    @teardown
    def my_teardown_task(cluster_id):
        return f"Tearing down resources {cluster_id}!"

    @task(task_id="extract", retries=2)
    def extract_bitcoin_price() -> dict[str, float]:
        return requests.get(API).json()["bitcoin"]

    @task(multiple_outputs=True)
    def process_data(response: dict[str, float]) -> dict[str, float]:
        logging.info(response)
        return {
            "usd": response["usd"],
            "change": response["usd_24h_change"],
        }

    @task
    def store_data(data: dict[str, float]):
        logging.info(f"Store: {data['usd']} with change {data['change']}")

    @task.bash
    def sleep_in(day: str) -> str:
        if day in (WeekDay.SATURDAY, WeekDay.SUNDAY):
            return f"sleep {2 * 2}"
        else:
            raise AirflowSkipException("No sleeping in today!")

    email_notification = EmailOperator(
        task_id="send_notify",
        to="noreply@email.io",
        subject="Dag Completed",
        html_content="the dag has finished",
    )

    extract = extract_bitcoin_price()
    t_setup = my_setup_task()

    # Notice that it also doesn't require using `ti.xcom_pull` and
    # `ti.xcom_push` to pass data between tasks. This is all handled
    # by the TaskFlow API when you define your task dependencies with;
    t_setup >> extract
    (
        store_data(
            process_data(
                extract,
            ),
        )
        >> sleep_in(day="{{ dag_run.logical_date.strftime('%A').lower() }}")
        >> email_notification
        >> my_teardown_task(t_setup)
    )


taskflow()
