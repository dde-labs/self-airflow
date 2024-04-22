from dataclasses import dataclass, field
from airflow.utils import dates


@dataclass(frozen=True)
class AirflowDag:
    """This dataclass contains all generic airflow variables."""

    description: str
    dag_id: str
    owner: str
    project: str
    doc_md: str
    # bigquery_location: str = "US"
    schedule: str | None = None
    # gcp_connection: str = "google_cloud_default"
    max_active_runs: int = 1
    retries: int = 1
    provide_context: bool = True
    tags: list = field(default_factory=list)
    catchup: bool = False
    email: list = field(default_factory=list)
    email_on_failure: bool = False
    start_date: dates = dates.days_ago(1)
