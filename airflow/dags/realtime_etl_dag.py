from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

# --- DAG definition ---
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 2, 15),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', os.getcwd())  # fallback to current dir


def run_producer():
    """Run the Kafka producer script"""
    producer_path = os.path.join(os.environ['AIRFLOW_HOME'], 'src', 'producers', 'produce_events.py')
    try:
        subprocess.run([sys.executable, producer_path], check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Producer failed: {e}")

def run_spark_job():
    """Run the Spark streaming job"""
    spark_job_path = os.path.join(os.environ['AIRFLOW_HOME'], 'spark_jobs', 'streaming_etl.py')
    try:
        subprocess.run(['spark-submit', '--verbose', spark_job_path], check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Spark job failed: {e}")


with DAG(
    'real_time_etl',
    default_args=default_args,
    schedule_interval=None,  # manual or trigger-based
    catchup=False,
    description="Real-time ETL: Kafka -> PySpark streaming"
) as dag:

    # --- Tasks ---
    start_kafka = BashOperator(
        task_id='start_kafka',
        bash_command='docker-compose -f /docker/docker-compose.yml up -d --build --remove-orphans'
    )

    start_producer = PythonOperator(
        task_id='start_producer',
        python_callable=run_producer
    )

    start_spark_job = PythonOperator(
        task_id='start_spark_job',
        python_callable=run_spark_job
    )

    stop_kafka = BashOperator(
        task_id='stop_kafka',
        bash_command='docker-compose -f /docker/docker-compose.yml down'
    )

    # --- DAG flow ---
    start_kafka >> start_producer >> start_spark_job >> stop_kafka
