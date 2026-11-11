from datetime import datetime
import subprocess

from airflow.sdk import dag, task

@dag(
    dag_id="urbanflow_pipeline",
    description="Pipeline de dados UrbanFlow: Bronze, Silver e Gold",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["urbanflow", "pyspark", "delta-lake"],
)
def urbanflow_pipeline():

    @task
    def ingest_bronze():
        subprocess.run(
            ["python", "-m", "src.ingestion.bronze"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def transform_silver():
        subprocess.run(
            ["python", "-m", "src.transformations.silver"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def transform_gold():
        subprocess.run(
            ["python", "-m", "src.transformations.gold"],
            cwd="/opt/airflow",
            check=True,
        )

    bronze = ingest_bronze()
    silver = transform_silver()
    gold = transform_gold()


    bronze >> silver >> gold


urbanflow_pipeline()