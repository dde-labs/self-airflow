from datetime import timedelta

from airflow import DAG
from airflow.utils.dag_cycle_tester import check_cycle

from dags.demo.demo_testing import dag as testing_dag


def test_demo_testing_dag_cycle():
    assert isinstance(testing_dag, DAG)
    check_cycle(testing_dag)


def test_demo_testing_dag_args():
    assert testing_dag.catchup is False
    assert testing_dag.default_args.get("owner") == "zkan"
    assert testing_dag.default_args.get("retries") == 3
    assert testing_dag.default_args.get("retry_delay") == timedelta(minutes=3)
