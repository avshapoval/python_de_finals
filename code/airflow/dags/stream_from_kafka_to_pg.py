import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from scripts.helpers.airflow_common import get_connection_uri

# Константы
JARS = "/opt/airflow/spark/jars/postgresql-42.2.18.jar"
PYSPARK_SCRIPT_PATH = '/opt/airflow/scripts/pyspark_scripts/stream_from_kafka_to_pg.py'

# Параметры по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

with DAG(
    'kafka_to_postgres_streaming',
    default_args=default_args,
    description='Стриминговое чтение пользователей из Kafka и запись в Postgres',
    schedule_interval=timedelta(days=1),
) as dag:
    # Креды и драйверы
    kafka_bootstrap_servers = os.getenv('KAFKA_INTERNAL_CONNECT_PATH', 'kafka:29092')
    kafka_topic = os.getenv('KAFKA_TOPIC_NAME', 'users-data')
    target_url = get_connection_uri(PostgresHook.get_connection('python_de_finals_postgresql'))
    target_driver = 'org.postgresql.Driver'
    target_table = 'users'

    # EmptyOperator для начала и конца DAG
    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    spark_submit_task = SparkSubmitOperator(
        task_id='spark_kafka_to_postgres',
        application=PYSPARK_SCRIPT_PATH,
        conn_id='python_de_finals_spark',
        application_args=[
            '--kafka_bootstrap_servers', kafka_bootstrap_servers,
            '--kafka_topic', kafka_topic,
            '--target_url', target_url,
            '--target_driver', target_driver,
            '--target_table', target_table
        ],
        conf={
            "spark.driver.memory": "700m",
            "spark.executor.memory": "700m"
        },
        jars=JARS,
        dag=dag,
        packages='org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3'
    )

    start >> spark_submit_task >> finish