from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
}

with DAG(
    dag_id='run_dbt_models',
    default_args=default_args,
    description='Run dbt models after raw data is loaded',
    schedule_interval=None,
    catchup=False
) as dag:

    dbt_build = BashOperator(
        task_id='dbt_build',
        bash_command='cd /opt/airflow/dbt_vg && dbt build'
    )