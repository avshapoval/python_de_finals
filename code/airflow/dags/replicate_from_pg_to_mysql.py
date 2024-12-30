from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Константы
TABLES = ["product_categories", "users", "products", "orders", "order_details"]
JARS = "/opt/airflow/spark/jars/postgresql-42.2.18.jar,/opt/airflow/spark/jars/mysql-connector-java-8.3.0.jar"

# Параметры по умолчанию для DAG
DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}


def get_connection_uri(conn):
    """Функция для создания валидного URI из параметров хука"""
    conn_type_jdbc_mapping = {
        "postgres": "postgresql",
        "mysql": "mysql"
    }
    conn_type = conn_type_jdbc_mapping[conn.conn_type]
    login = conn.login
    password = conn.password
    host = conn.host
    port = conn.port
    db = conn.schema
    extras_list = [f"{k}={v}" for k, v in conn.extra_dejson.items()]
    extras = f"&{'&'.join(extras_list)}" if extras_list else ''
    return f"jdbc:{conn_type}://{host}:{port}/{db}?user={login}&password={password}{extras}"


with DAG(
    'replicate_from_pg_to_mysql',
    default_args=DEFAULT_DAG_ARGS,
    description='Replicate data from PostgreSQL to MySQL using Spark',
    schedule_interval=timedelta(days=1),
) as dag:
    # DummyOperator для начала и конца DAG
    start = EmptyOperator(
        task_id='start'
    )
    finish = EmptyOperator(
        task_id='finish'
    )

    for table in TABLES:
        # Параметры подключения
        source_url = get_connection_uri(PostgresHook.get_connection('python_de_finals_postgresql'))
        source_driver = 'org.postgresql.Driver'
        target_url = get_connection_uri(MySqlHook.get_connection('python_de_finals_mysql'))
        target_driver = 'com.mysql.cj.jdbc.Driver'

        spark_submit_task = SparkSubmitOperator(
            task_id=f'replicate_{table}',
            application='/opt/airflow/scripts/replicate_table_by_spark.py',
            conn_id='python_de_finals_spark',
            application_args=[
                '--source_url', source_url,
                '--source_driver', source_driver,
                '--target_url', target_url,
                '--target_driver', target_driver,
                '--table', table
            ],
            conf={
                "spark.driver.memory": "500m",
                "spark.executor.memory": "500m"
            },
            dag=dag,
            jars=JARS
        )

        start >> spark_submit_task >> finish