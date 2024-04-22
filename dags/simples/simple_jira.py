from datetime import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.atlassian.jira.notifications.jira import send_jira_notification
from airflow.providers.http.operators.http import HttpOperator

"""
Connection:
jira_default:
    conn_type: jira
    host: https://<account-name>.atlassian.net
    login: <corporate-email>
    password: <token>
"""


@dag(
    dag_id="simple_jira_dag",
    start_date=datetime(2023, 11, 3),
    on_failure_callback=[
        send_jira_notification(
            jira_conn_id="my-jira-conn",
            description="Failure in the DAG {{ dag.dag_id }}",
            summary="Airflow DAG Issue",
            project_id=10000,
            issue_type_id=10003,
            labels=["airflow-dag-failure"],
        )
    ],
)
def simple_jira_dag():
    task_get_op = HttpOperator(
        task_id="get_jira",
        method="GET",
        endpoint="https://<jira>",
        data={"param1": "value1", "param2": "value2"},
        headers={"Content-Type": "application/json"},
        # response_check=lambda response: response.json()["json"]["priority"] == 5,
    )

    fail_task = BashOperator(
        task_id="fail_task",
        on_failure_callback=[
            send_jira_notification(
                jira_conn_id="my-jira-conn",
                description="The task {{ ti.task_id }} failed",
                summary="Airflow Task Issue",
                project_id=10000,
                issue_type_id=10003,
                labels=["airflow-task-failure"],
            )
        ],
        bash_command="fail",
        retries=0,
    )

    task_get_op >> fail_task
