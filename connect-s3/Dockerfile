FROM confluentinc/cp-kafka-connect-base:7.6.0

USER root

RUN curl -fsSL \
        "https://github.com/databricks/iceberg-kafka-connect/releases/download/v0.6.19/iceberg-kafka-connect-runtime-0.6.19.zip" \
        -o /tmp/iceberg-kafka-connect.zip && \
    python3 -c "import zipfile; zipfile.ZipFile('/tmp/iceberg-kafka-connect.zip').extractall('/usr/share/confluent-hub-components/')" && \
    rm /tmp/iceberg-kafka-connect.zip

USER 1000
