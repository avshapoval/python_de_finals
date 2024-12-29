FROM apache/airflow:2.10.4-python3.10

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

# Install modules for spark
USER airflow
COPY ./requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Entrypoint
CMD ["scheduler"]