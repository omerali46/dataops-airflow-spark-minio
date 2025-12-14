from datetime import datetime
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator

REPO_PATH = "/git/repo"
VAR_KEY = "last_processed_git_sha"

def _sha() -> str:
    return subprocess.check_output(
        ["bash", "-lc", f"cd {REPO_PATH} && git rev-parse HEAD"],
        text=True
    ).strip()

def should_run(**_):
    current = _sha()
    last = Variable.get(VAR_KEY, default_var="")
    return current != last

def save_sha(**_):
    Variable.set(VAR_KEY, _sha())

with DAG(
    dag_id="store_transactions_full_load",
    start_date=datetime(2025, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    tags=["dataops"],
) as dag:

    check_change = ShortCircuitOperator(
        task_id="check_main_commit_changed",
        python_callable=should_run,
    )

    run_job = SSHOperator(
        task_id="run_job_on_spark_client",
        ssh_conn_id="spark_ssh",
        command="python3 /git/repo/jobs/clean_transactions.py",
        get_pty=True,
    )

    mark_done = PythonOperator(
        task_id="save_processed_sha",
        python_callable=save_sha,
    )

    check_change >> run_job >> mark_done
