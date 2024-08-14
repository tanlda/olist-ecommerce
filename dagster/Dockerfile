FROM maven:3.8.5-openjdk-17 AS builder

COPY pom.xml pom.xml

RUN mvn -f pom.xml clean package dependency:copy-dependencies

FROM spark:python3-java17

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates wget gcc build-essential libpq-dev && \
    apt-get clean

ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV DAGSTER_APP=/opt/dagster/app
RUN mkdir -p $DAGSTER_HOME $DAGSTER_APP
WORKDIR $DAGSTER_APP

COPY ./requirements.txt ./requirements.txt

RUN pip install --upgrade pip && pip install -r requirements.txt

COPY --from=builder /target/dependency/ /opt/spark/jars

COPY ./storage /opt/dagster/dagster_home/storage/

COPY ./ecommerce ./ecommerce

COPY ./data ./data

EXPOSE 4000

ENV SPARK_VERSION=3.5

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "ecommerce"]

#USER 185
