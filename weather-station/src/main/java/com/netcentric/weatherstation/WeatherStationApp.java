package com.netcentric.weatherstation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netcentric.weatherstation.model.Weather;
import com.netcentric.weatherstation.model.WeatherStatusMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Mocks a single weather station that samples its sensors every second and
 * publishes a JSON status message to a Kafka topic.
 *
 * Configuration is fully environment-driven so the same image can be reused
 * for all 10 station pods/containers (only STATION_ID changes).
 */
public class WeatherStationApp {

    private static final Logger log = LoggerFactory.getLogger(WeatherStationApp.class);

    // battery_status distribution: low 30%, medium 40%, high 30%
    private static final double LOW_THRESHOLD = 0.30;
    private static final double MEDIUM_THRESHOLD = 0.70; // 0.30 + 0.40

    // 10% of sampled messages are dropped (never sent to Kafka)
    private static final double DROP_RATE = 0.10;

    public static void main(String[] args) {
        long stationId = Long.parseLong(getEnv("STATION_ID", "1"));
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");
        String topic = getEnv("WEATHER_TOPIC", "weather-readings");
        long intervalMs = Long.parseLong(getEnv("EMIT_INTERVAL_MS", "1000"));

        log.info("Starting weather station {} -> topic '{}' via {}", stationId, topic, bootstrapServers);

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "50");

        ObjectMapper mapper = new ObjectMapper();
        ThreadLocalRandom random = ThreadLocalRandom.current();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> log.info("Station {} shutting down", stationId)));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            long sNo = 0;
            while (true) {
                sNo++; // sequence number increments for every sampled reading, sent or not

                String batteryStatus = randomBatteryStatus(random);
                Weather weather = new Weather(
                        randomHumidity(random),
                        randomTemperatureF(random),
                        randomWindSpeed(random)
                );
                WeatherStatusMessage message = new WeatherStatusMessage(
                        stationId,
                        sNo,
                        batteryStatus,
                        System.currentTimeMillis() / 1000L,
                        weather
                );

                boolean dropped = random.nextDouble() < DROP_RATE;
                if (!dropped) {
                    try {
                        String json = mapper.writeValueAsString(message);
                        ProducerRecord<String, String> record =
                                new ProducerRecord<>(topic, String.valueOf(stationId), json);
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                log.error("Failed to send message for station {}", stationId, exception);
                            }
                        });
                    } catch (Exception e) {
                        log.error("Failed to serialize/send message", e);
                    }
                } else {
                    log.debug("Station {} dropped message s_no={}", stationId, sNo);
                }

                Thread.sleep(intervalMs);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Station {} interrupted, exiting", stationId);
        }
    }

    private static String randomBatteryStatus(ThreadLocalRandom random) {
        double r = random.nextDouble();
        if (r < LOW_THRESHOLD) {
            return "low";
        } else if (r < MEDIUM_THRESHOLD) {
            return "medium";
        } else {
            return "high";
        }
    }

    private static int randomHumidity(ThreadLocalRandom random) {
        // 0-100%, biased range widened around 40-90 so rain (>70%) triggers occasionally
        return random.nextInt(20, 101);
    }

    private static int randomTemperatureF(ThreadLocalRandom random) {
        return random.nextInt(20, 121); // Fahrenheit
    }

    private static int randomWindSpeed(ThreadLocalRandom random) {
        return random.nextInt(0, 61); // km/h
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }
}
