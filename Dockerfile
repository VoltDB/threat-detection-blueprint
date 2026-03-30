# Build stage
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src/ src/
RUN mvn clean package -DskipTests -B

# Runtime stage
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/threat-detection-blueprint-1.0.jar app.jar
COPY --from=build /build/target/threat-detection-blueprint-1.0-procedures.jar procedures.jar
COPY --from=build /build/target/lib/ lib/

ENV VOLTDB_HOST=localhost
ENV VOLTDB_PORT=21211
ENV CIDR_PREFIX=24
ENV GCP_PROJECT=<your-gcp-project>
ENV PUBSUB_TOPIC=threat-transactions
ENV SEND_TO_PUBSUB=true

ENTRYPOINT ["sh", "-c", \
  "java -Dcidr.prefix=${CIDR_PREFIX} \
        -Dgcp.project=${GCP_PROJECT} \
        -Dpubsub.topic.transactions=${PUBSUB_TOPIC} \
        -DsendToPubSub=${SEND_TO_PUBSUB} \
        -Dprocedures.jar.path=/app/procedures.jar \
        -cp app.jar:lib/* \
        com.example.voltdb.ThreatDetectionApp \
        ${VOLTDB_HOST} ${VOLTDB_PORT}"]
