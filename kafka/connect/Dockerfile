FROM confluentinc/cp-kafka-connect:7.7.0 as builder

ENV PLUGIN_DIR="/usr/share/confluent-hub-components"
ENV CONNECT_PLUGIN_PATH="/usr/share/java,$PLUGIN_DIR"

USER root

RUN curl -L -o opensearch-connector.tar https://github.com/Aiven-Open/opensearch-connector-for-apache-kafka/releases/download/v3.1.1/opensearch-connector-for-apache-kafka-3.1.1.tar && \
    mkdir -p $PLUGIN_DIR/aiven-opensearch-connector && tar -xvf opensearch-connector.tar -C $PLUGIN_DIR/aiven-opensearch-connector --strip-components=1 && \
    rm opensearch-connector.tar

USER appuser

RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:2.2.1
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:7.6.0
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.5
RUN confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:1.13.0 && \
    curl -L -o /usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb/lib/avro-1.11.3.jar https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.jar

FROM quay.io/strimzi/kafka:latest-kafka-3.7.0

USER root:root

RUN mkdir -p /opt/kafka/plugins/kafka-connect-jdbc/
RUN mkdir -p /opt/kafka/plugins/kafka-connect-mongodb/
RUN mkdir -p /opt/kafka/plugins/kafka-connect-avro-converter/
RUN mkdir -p /opt/kafka/plugins/debezium-connector-postgresql/

COPY --from=builder /usr/share/confluent-hub-components/mongodb-kafka-connect-mongodb /opt/kafka/plugins/kafka-connect-mongodb/
COPY --from=builder /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc /opt/kafka/plugins/kafka-connect-jdbc/
COPY --from=builder /usr/share/confluent-hub-components/confluentinc-kafka-connect-avro-converter/lib /opt/kafka/plugins/kafka-connect-avro-converter/
COPY --from=builder /usr/share/confluent-hub-components/debezium-debezium-connector-postgresql /opt/kafka/plugins/debezium-connector-postgresql/
COPY --from=builder /usr/share/confluent-hub-components/aiven-opensearch-connector /opt/kafka/plugins/aiven-opensearch-connector/

USER 1001

