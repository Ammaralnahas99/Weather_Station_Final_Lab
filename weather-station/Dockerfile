FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
RUN apk add --no-cache bash
COPY --from=build /build/target/weather-station.jar app.jar
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    WEATHER_TOPIC=weather-readings \
    EMIT_INTERVAL_MS=1000

ENTRYPOINT ["/app/entrypoint.sh"]
