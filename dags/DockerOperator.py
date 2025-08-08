from airflow.operators.docker_operator import DockerOperator
from airflow import DAG
from datetime import datetime

with DAG(
    dag_id="airbnb_pipeline",
    start_date=datetime(2025, 8, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract_task = DockerOperator(
        task_id='extract_airbnb_data',
        image='usuario_dockerhub/airbnb-extractor:1.0',
        command='python scripts/ingestion/extract_airbnb_data.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        volumes=['/ruta/local/data:/app/data'],
    )
