
from os.path import expanduser
from pathlib import Path
import os
home = expanduser("~")
airflow_dir = os.path.join(home, 'airflow')
Path(f"{airflow_dir}/dags").mkdir(parents=True, exist_ok=True)

from airflow import *
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    dag_id='0eaa2e755d1f_dag',
    default_args=default_args,
    description='DAG to run Databricks notebook daily',
    schedule_interval='@daily',  # Runs daily
    start_date=datetime(2024, 8, 29),
    catchup=False
)

# Define the Databricks notebook run task
run_notebook = DatabricksRunNowOperator(
    task_id='run_databricks_notebook',
    job_id='<0eaa2e755d1f_job>',  # Replace with your Databricks job ID
    databricks_conn_id='databricks_default',  # Ensure you have this connection set up
    dag=dag
)

# Set task dependencies
run_notebook