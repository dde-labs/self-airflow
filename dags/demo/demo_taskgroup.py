from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.decorators import dag, task_group


default_args = {
    "owner": "korawica",
    "start_date": timezone.datetime(2022, 2, 1),
}


@dag(
    "demo_task_group",
    default_args=default_args,
    schedule_interval=None,
)
def demo_task_group():
    start = EmptyOperator(task_id="start")

    @task_group(
        group_id="demo_group",
        # default_args={"conn_id": "postgres_default"},
        tooltip="This task group is very important!",
        prefix_group_id=True,
        # parent_group=None,
        # dag=None,
    )
    def task_group_1():
        t1 = EmptyOperator(task_id="t1")
        t2 = EmptyOperator(task_id="t2")

        t1 >> t2

    end = EmptyOperator(task_id="end")

    start >> task_group_1() >> end


demo_task_group()
