FROM apache/airflow:2.10.4-python3.10

COPY setup_airflow.sh /opt/setup_scripts/setup_airflow.sh

USER root
RUN mkdir -p /usr/local/airflow/db /opt/airflow/dags /opt/airflow/scripts
RUN chown "$AIRFLOW_UID:0" /opt/setup_scripts/setup_airflow.sh
RUN chown "$AIRFLOW_UID:0" /usr/local/airflow/db
RUN chown "$AIRFLOW_UID:0" /opt/airflow/dags
RUN chown "$AIRFLOW_UID:0" /opt/airflow/scripts

USER airflow

# Entrypoint
CMD ["bash", "-c", "/opt/setup_scripts/setup_airflow.sh"]