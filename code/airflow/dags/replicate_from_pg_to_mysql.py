from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

from scripts.helpers.airflow_common import get_connection_uri

# Константы
TABLES = ["product_categories", "users", "products", "orders", "order_details"]
JARS = "/opt/airflow/spark/jars/postgresql-42.2.18.jar,/opt/airflow/spark/jars/mysql-connector-java-8.3.0.jar"
PYSPARK_REPLICATION_SCRIPT_PATH = f'/opt/airflow/scripts/pyspark_scripts/replicate_table_by_spark.py'

# Параметры по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

with DAG(
    'replicate_from_pg_to_mysql',
    default_args=default_args,
    description='Репликация данных из PG в MySQL через Spark',
    schedule_interval=timedelta(days=1),
) as dag:
    # Креды и драйверы
    source_url = get_connection_uri(PostgresHook.get_connection('python_de_finals_postgresql'))
    source_driver = 'org.postgresql.Driver'
    target_url = get_connection_uri(MySqlHook.get_connection('python_de_finals_mysql'))
    target_driver = 'com.mysql.cj.jdbc.Driver'

    # EmptyOperator для начала и конца DAG
    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    for table in TABLES:
        # Параметры подключения
        spark_submit_task = SparkSubmitOperator(
            task_id=f'replicate_{table}',
            application=PYSPARK_REPLICATION_SCRIPT_PATH,
            conn_id='python_de_finals_spark',
            application_args=[
                '--source_url', source_url,
                '--source_driver', source_driver,
                '--target_url', target_url,
                '--target_driver', target_driver,
                '--table', table
            ],
            conf={
                "spark.driver.memory": "700m",
                "spark.executor.memory": "700m"
            },
            jars=JARS
        )

        start >> spark_submit_task >> finish