FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /build/target/central-station.jar app.jar

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    WEATHER_TOPIC=weather-readings \
    RAIN_ALERTS_TOPIC=rain-alerts \
    DB_HOST=postgres \
    DB_PORT=5432 \
    DB_NAME=weather \
    DB_SSLMODE=prefer

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
