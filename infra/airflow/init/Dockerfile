FROM apache/airflow:2.10.4-python3.10

COPY setup_airflow.sh /opt/scripts/setup_airflow.sh

USER root
RUN mkdir -p /usr/local/airflow/db /opt/airflow/dags
RUN chown "$AIRFLOW_UID:0" /opt/scripts/setup_airflow.sh
RUN chown "$AIRFLOW_UID:0" /usr/local/airflow/db
RUN chown "$AIRFLOW_UID:0" /opt/airflow/dags

USER airflow

# Entrypoint
CMD ["bash", "-c", "/opt/scripts/setup_airflow.sh"]