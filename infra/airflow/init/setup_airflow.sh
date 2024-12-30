#!/bin/bash

# Запуск Airflow
airflow db migrate

# Создание юзеров
airflow users create \
    --username admin \
    --password admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org

# Создание коннектов
airflow connections add 'python_de_finals_postgresql' \
    --conn-json '{
        "conn_type": "postgres",
        "login": "'"$POSTGRESQL_APP_USER"'",
        "password": "'"$POSTGRESQL_APP_PASSWORD"'",
        "host": "'"$POSTGRESQL_APP_HOST"'",
        "port": 5432,
        "schema": "'"$POSTGRESQL_APP_DB"'",
        "extra": {
            "currentSchema": "'"$POSTGRESQL_APP_SCHEMA"'"
        }
    }'

airflow connections add 'python_de_finals_mysql' \
    --conn-json '{
        "conn_type": "mysql",
        "login": "'"$MYSQL_APP_USER"'",
        "password": "'"$MYSQL_APP_PASSWORD"'",
        "host": "'"$MYSQL_APP_HOST"'",
        "port": 3306,
        "schema": "'"$MYSQL_APP_DB"'"
    }'

airflow connections add 'python_de_finals_spark' \
    --conn-json '{
        "conn_type": "generic",
        "host": "spark://'"$SPARK_MASTER_HOST"'",
        "port": "'"$SPARK_MASTER_PORT"'",
        "extra": {
            "deploy-mode": "client",
            "spark_binary": "spark3-submit"
        }
    }'

airflow version