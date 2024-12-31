from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.mysql.hooks.mysql import MySqlHook

from scripts.helpers.airflow_common import get_connection_uri

# Константы
ANALYTICAL_MARTS = ["user_activity", "product_sales"]
JARS = "/opt/airflow/spark/jars/mysql-connector-java-8.3.0.jar"
PYSPARK_SCRIPT_PATH = '/opt/airflow/scripts/pyspark_scripts/create_analytical_views.py'

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
    'create_analytical_views',
    default_args=default_args,
    description='Создание витрин данных в MySQL через Spark',
    schedule_interval=timedelta(days=1),
) as dag:
    # Креды и драйверы
    src_tgt_url = get_connection_uri(MySqlHook.get_connection('python_de_finals_mysql'))
    src_tgt_driver = 'com.mysql.cj.jdbc.Driver'

    # EmptyOperator для начала и конца DAG
    start = EmptyOperator(task_id='start')
    finish = EmptyOperator(task_id='finish')

    for mart in ANALYTICAL_MARTS:
        spark_submit_task = SparkSubmitOperator(
            task_id=f'create_mart_{mart}',
            application=PYSPARK_SCRIPT_PATH,
            conn_id='python_de_finals_spark',
            application_args=[
                '--src_tgt_url', src_tgt_url,
                '--src_tgt_driver', src_tgt_driver,
                '--target_mart', mart
            ],
            conf={
                "spark.driver.memory": "700m",
                "spark.executor.memory": "700m"
            },
            jars=JARS
        )

        start >> spark_submit_task >> finish