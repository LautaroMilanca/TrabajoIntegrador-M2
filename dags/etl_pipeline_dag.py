from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2025, 8, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract_task = BashOperator(
        task_id="extract_data",
        bash_command="python /scripts/ingestion/extract_airbnb_data.py"
    )

    transform_task = BashOperator(
        task_id="transform_data",
        bash_command="dbt run --project-dir /dbt"
    )

    load_task = BashOperator(
        task_id="load_data",
        bash_command="python /scripts/load/load_to_dwh.py"
    )

    extract_task >> transform_task >> load_task
