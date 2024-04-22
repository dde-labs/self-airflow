import logging
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.email import EmailOperator
from airflow.utils import timezone


def retry_callback(context):
    logging.info(f"{context}")
    logging.error("Cannot call API at this moment. Wait for retrying...")


def failure_callback(context):
    logging.info(f"{context}")
    logging.error("Cannot call API. Please contact the admin.")


default_args = {
    "owner": "korawica",
    "email": ["korawica@email.io"],
    "email_on_failure": True,
    "email_on_retry": False,
    "start_date": timezone.datetime(2024, 4, 1),
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "on_retry_callback": retry_callback,
    "on_failure_callback": failure_callback,
}
with DAG(
    "demo_retry_and_alert",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    @task(
        task_id="get_data",
        retries=3,
        pool="default_pool",
    )
    def get_data_from_api():
        logging.info("Start getting data from API")
        raise ValueError()

    notify = EmailOperator(
        task_id="send_notify",
        to=["korawica@email.io"],
        subject="Some subject",
        html_content="""Some content""",
    )

    (
        get_data_from_api.override(
            retries=0,
            pool="default_pool",
            task_id="get_data_from_api",
        )()
        >> notify
    )

    # notify.set_upstream(get_data_from_api)
