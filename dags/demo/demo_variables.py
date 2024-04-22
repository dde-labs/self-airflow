import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils import timezone


def _get_var():
    foo = Variable.get("gd_prefix", default_var=None)
    logging.info(foo)

    bar = Variable.get(
        "demo_json",
        deserialize_json=True,
        default_var=None,
    )
    logging.info(bar)


default_args = {
    "owner": "korawica",
    "start_date": timezone.datetime(2024, 2, 1),
}
with DAG(
    "demo_variables",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    get_var = PythonOperator(
        task_id="get_var",
        python_callable=_get_var,
    )

    get_var_bash = BashOperator(
        task_id="get_var_bash",
        bash_command="echo {{ var.value.get('foo') }}",
    )

    get_var >> get_var_bash
