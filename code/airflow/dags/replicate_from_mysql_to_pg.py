from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'replicate_from_pg_to_mysql',
    default_args=default_args,
    description='Replicate data from PostgreSQL to MySQL using Spark',
    schedule_interval=timedelta(days=1),
)

replicate_task = SparkSubmitOperator(
    task_id='replicate_data',
    application='/usr/local/airflow/scripts/replicate_from_pg_to_mysql.py',
    conn_id='spark_default',
    dag=dag,
)