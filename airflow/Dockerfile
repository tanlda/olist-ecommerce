FROM apache/airflow:latest-python3.10

USER root

RUN apt-get update -y && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/

RUN export JAVA_HOME

USER airflow

COPY ./requirements.txt ./requirements.txt

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

COPY ./dags /opt/airflow/dags

COPY ./plugins /opt/airflow/plugins
