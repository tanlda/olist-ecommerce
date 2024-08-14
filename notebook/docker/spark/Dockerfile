FROM maven:3.8.5-openjdk-17 AS builder

COPY pom.xml pom.xml

RUN mvn -f pom.xml clean package dependency:copy-dependencies

FROM quay.io/jupyter/pyspark-notebook

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates unzip wget curl gcc build-essential libpq-dev && \
    apt-get clean

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscli.zip"
RUN unzip awscli.zip && ./aws/install && rm awscli.zip

USER jovyan

RUN pip install boto3 polars pyarrow tqdm unidecode minio pyiceberg sqlalchemy
RUN pip install psycopg2 parsel duckdb faker pyiceberg[glue,s3fs] trino pymongo pydeequ

COPY --from=builder /target/dependency/ /usr/local/spark/jars/

EXPOSE 8888

WORKDIR /home/jovyan/work

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--NotebookApp.token=''", "--no-browser"]
