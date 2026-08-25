package com.netcentric.centralstation;

import com.netcentric.centralstation.consumer.RainAlertsConsumer;
import com.netcentric.centralstation.consumer.WeatherReadingsConsumer;
import com.netcentric.centralstation.db.DbConfig;
import com.netcentric.centralstation.db.DbWriter;
import com.netcentric.centralstation.streams.RainDetectionTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CentralStationApp {

    private static final Logger log = LoggerFactory.getLogger(CentralStationApp.class);

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");
        String weatherTopic = getEnv("WEATHER_TOPIC", "weather-readings");
        String rainTopic = getEnv("RAIN_ALERTS_TOPIC", "rain-alerts");

        log.info("Central Station starting up. Kafka={}", bootstrapServers);

        DbWriter dbWriter = new DbWriter(DbConfig.fromEnv());

        RainDetectionTopology rainTopology = new RainDetectionTopology(bootstrapServers, weatherTopic, rainTopic);
        rainTopology.start();

        WeatherReadingsConsumer readingsConsumer = new WeatherReadingsConsumer(bootstrapServers, weatherTopic, dbWriter);
        Thread readingsThread = new Thread(readingsConsumer, "weather-readings-consumer");
        readingsThread.start();

        RainAlertsConsumer alertsConsumer = new RainAlertsConsumer(bootstrapServers, rainTopic);
        Thread alertsThread = new Thread(alertsConsumer, "rain-alerts-consumer");
        alertsThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Central Station shutting down");
            readingsConsumer.stop();
            alertsConsumer.stop();
            rainTopology.close();
            try {
                readingsThread.join(SHUTDOWN_JOIN_TIMEOUT_MS);
                alertsThread.join(SHUTDOWN_JOIN_TIMEOUT_MS);
                dbWriter.close();
            } catch (Exception e) {
                log.warn("Error during shutdown", e);
            }
        }));

        readingsThread.join();
    }

    private static final long SHUTDOWN_JOIN_TIMEOUT_MS = 10_000L;

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }
}
