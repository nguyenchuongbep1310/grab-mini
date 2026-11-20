# One image for every Flink service of the demo (JobManager, TaskManager and
# the one-shot SQL submit). The two jars are the only additions to the stock
# image: the Kafka connector and the Avro format for Schema Registry. Both are
# pinned to the Flink minor (1.20) and checked against their published SHA-1
# from Maven Central, so a changed artifact fails the build.
FROM flink:1.20.5

ARG MAVEN=https://repo1.maven.org/maven2/org/apache/flink
ARG KAFKA_JAR=flink-sql-connector-kafka-3.4.0-1.20.jar
ARG KAFKA_SHA1=266330f8f26fec4957339b5ff31aa67b81c31462
ARG AVRO_JAR=flink-sql-avro-confluent-registry-1.20.5.jar
ARG AVRO_SHA1=abca6af761dbe802868ca9e77b4a40ac5a491c12

RUN set -eux; \
    curl -fsSL -o "/opt/flink/lib/${KAFKA_JAR}" "${MAVEN}/flink-sql-connector-kafka/3.4.0-1.20/${KAFKA_JAR}"; \
    curl -fsSL -o "/opt/flink/lib/${AVRO_JAR}" "${MAVEN}/flink-sql-avro-confluent-registry/1.20.5/${AVRO_JAR}"; \
    echo "${KAFKA_SHA1}  /opt/flink/lib/${KAFKA_JAR}" | sha1sum -c -; \
    echo "${AVRO_SHA1}  /opt/flink/lib/${AVRO_JAR}" | sha1sum -c -; \
    mkdir -p /opt/flink/checkpoints; \
    chown -R flink:flink /opt/flink/checkpoints /opt/flink/lib
