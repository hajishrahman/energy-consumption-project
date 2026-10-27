from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

STEPS = [
    ("ingest", "src/ingest.py"),
    ("silver", "src/silver.py"),
    ("process", "src/process.py"),
    ("predict", "src/predict.py"),
    ("dashboard", "src/dashboard.py"),
]

default_args = {
    'owner': 'hajish',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'energy_consumption_pipeline',
    default_args=default_args,
    description='PySpark Energy Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['energy', 'pyspark'],
) as dag:

    previous_task = None
    
    for task_id, script in STEPS:
        current_task = BashOperator(
            task_id=task_id,
            bash_command=f'cd /opt/airflow && python {script}',
            env={**os.environ}
        )
        
        if previous_task:
            previous_task >> current_task
        
        previous_task = current_task