FROM maven:3.8.5-openjdk-17 AS builder

COPY pom.xml pom.xml

RUN mvn -f pom.xml clean package dependency:copy-dependencies

FROM flink:1.18.1-java17

# Install dependencies
RUN apt-get update && \
    apt-get install -y software-properties-common ca-certificates wget gcc build-essential libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

# Install python 3.10
RUN add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.10 python3.10-dev ython3.10-venv && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1 && \
    rm -rf /var/lib/apt/lists/*

# Install pip
RUN curl https://bootstrap.pypa.io/get-pip.py | python3.10 && \
    rm -rf /var/lib/apt/lists/*

# Install python packages
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install apache-flink==1.18.1

# Copy dependencies
COPY --from=builder /target/dependency/ /opt/flink/lib/

WORKDIR /opt/flink

USER flink
RUN mkdir /opt/flink/usrlib
ADD main.py /opt/flink/usrlib/main.py
