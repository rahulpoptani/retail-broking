FROM maven:3.9.9-eclipse-temurin-17 AS iceberg-plugin

ARG ICEBERG_VERSION=1.11.0
WORKDIR /build

COPY pom.xml .

RUN mvn -q -Diceberg.version="${ICEBERG_VERSION}" \
    org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory=/iceberg-kafka-connect

FROM confluentinc/cp-kafka-connect-base:7.7.0

USER root

COPY --from=iceberg-plugin \
    /iceberg-kafka-connect \
    /usr/share/confluent-hub-components/iceberg-kafka-connect

USER 1000
