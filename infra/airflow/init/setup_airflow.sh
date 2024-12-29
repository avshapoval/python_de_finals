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
airflow connections add \
    --conn-type 'postgres' \
    --conn-host "$POSTGRESQL_APP_HOST" \
    --conn-login "$POSTGRESQL_APP_USER" \
    --conn-password "$POSTGRESQL_APP_PASSWORD" \
    --conn-schema "$POSTGRESQL_APP_DB" \
    --conn-port '5432' \
    'python_de_finals_postgresql'

airflow connections add \
    --conn-type 'mysql' \
    --conn-host "$MYSQL_HOST" \
    --conn-login "$MYSQL_APP_USER" \
    --conn-password "$MYSQL_APP_PASSWORD" \
    --conn-schema "$MYSQL_DATABASE" \
    --conn-port '3306' \
    'python_de_finals_mysql'

airflow version