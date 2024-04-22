import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


"""
Read more: https://blog.min.io/apache-airflow-minio/
"""


default_args = {
    "owner": "korawica",
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    "provide_context": True,
}


@dag(
    dag_id="simple_minio",
    tags=["minio"],
    default_args=default_args,
    schedule=None,
    # start_date=days_ago(2),
    # schedule_interval='0 * * * *',
    catchup=False,
)
def minio_dag():

    @task
    def task_s3_get_content():
        s3 = S3Hook(
            aws_conn_id="minio_local",
            transfer_config_args={
                "use_threads": False,
            },
        )
        assert s3.check_for_bucket("airflow-data")
        obj = s3.get_key("file.json", "airflow-data")
        contents = obj.get()["Body"].read().decode("utf-8")
        logging.info(f"Contents: \n{contents}")

    task_s3_get_content()


minio_dag()
