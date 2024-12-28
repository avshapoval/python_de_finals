FROM apache/airflow:2.1.2

COPY config/airflow.cfg /usr/local/airflow/airflow.cfg
COPY dags /usr/local/airflow/dags

USER root
RUN chown -R airflow: /usr/local/airflow

USER airflow