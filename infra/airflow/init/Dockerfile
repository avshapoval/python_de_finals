# На основе гайда https://airflow.apache.org/docs/apache-airflow/2.10.4/start.html
# Ниже Dockerfile для сборки standalone airflow, собирать все компоненты по отдельности - это слишком в плане использования ресурсов
FROM apache/airflow:2.10.4-python3.10

# Cкрипт запуска airflow
USER root
COPY setup_airflow.sh /opt/scripts/setup_airflow.sh
RUN chmod 777 /opt/scripts/setup_airflow.sh
USER airflow

# Entrypoint
CMD ["bash", "-c", "/opt/scripts/setup_airflow.sh"]