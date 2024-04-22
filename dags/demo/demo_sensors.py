from airflow.decorators import dag
from airflow.sensors.filesystem import FileSensor
from airflow.utils import timezone


default_args = {
    "owner": "korawica",
    "start_date": timezone.datetime(2024, 2, 1),
}


@dag(
    dag_id="demo_sensors",
    default_args=default_args,
    schedule_interval=None,
)
def demo_sensors():
    _ = FileSensor(
        task_id="is_file_available",
        fs_conn_id="local_data",
        filepath="local/file.json",
        poke_interval=5,
        timeout=20,
    )


demo_sensors()
