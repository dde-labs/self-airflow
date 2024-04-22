from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.smtp.notifications.smtp import send_smtp_notification


with DAG(
    dag_id="smtp_notifier",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    on_failure_callback=[
        send_smtp_notification(
            smtp_conn_id="smtp_mailhog",
            from_email="someone@mail.com",
            to="someone@mail.com",
            subject="[Error] The dag {{ dag.dag_id }} failed",
            html_content="debug logs",
        ),
    ],
):
    BashOperator(
        task_id="mytask",
        on_failure_callback=[
            send_smtp_notification(
                smtp_conn_id="smtp_mailhog",
                from_email="someone@mail.com",
                to="someone@mail.com",
                subject="[Error] The Task {{ ti.task_id }} failed",
                html_content="debug logs",
            ),
        ],
        bash_command="fail",
    )
