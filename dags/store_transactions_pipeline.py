from datetime import datetime
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

with DAG(
    dag_id="store_transactions_full_load",
    start_date=datetime(2025, 1, 1),
    schedule=None,   # sadece manuel tetiklenecek (ödev için net)
    catchup=False,
    tags=["dataops"],
) as dag:

    run_job = SSHOperator(
        task_id="run_job_on_spark_client",
        ssh_conn_id="spark_ssh",
        command="python3 /git/repo/jobs/clean_transactions.py",
        get_pty=True,
    )
