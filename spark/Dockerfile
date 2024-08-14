FROM maven:3.8.5-openjdk-17 AS builder

COPY pom.xml pom.xml

RUN mvn -f pom.xml clean package dependency:copy-dependencies

FROM spark:python3-java17

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates wget gcc build-essential libpq-dev && \
    apt-get clean

WORKDIR /opt/spark/work-dir

COPY ./requirements.txt ./requirements.txt

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

COPY --from=builder /target/dependency/ /opt/spark/jars

COPY ./tasks ./tasks

#USER 185
