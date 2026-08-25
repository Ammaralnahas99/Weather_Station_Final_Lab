package com.netcentric.centralstation.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netcentric.centralstation.model.WeatherStatusMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class RainDetectionTopology {

    private static final Logger log = LoggerFactory.getLogger(RainDetectionTopology.class);
    private static final int RAIN_HUMIDITY_THRESHOLD = 70;

    private final KafkaStreams streams;

    public RainDetectionTopology(String bootstrapServers, String inputTopic, String outputTopic) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detection-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = buildTopology(inputTopic, outputTopic);
        this.streams = new KafkaStreams(topology, props);
    }

    static Topology buildTopology(String inputTopic, String outputTopic) {
        ObjectMapper mapper = new ObjectMapper();
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> readings = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        readings
                .filter((key, value) -> isRaining(mapper, value))
                .mapValues(value -> toRainAlertJson(mapper, value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    private static boolean isRaining(ObjectMapper mapper, String json) {
        try {
            WeatherStatusMessage msg = mapper.readValue(json, WeatherStatusMessage.class);
            return msg.getWeather() != null && msg.getWeather().getHumidity() > RAIN_HUMIDITY_THRESHOLD;
        } catch (Exception e) {
            log.warn("Skipping unparsable message in rain detector: {}", e.getMessage());
            return false;
        }
    }

    private static String toRainAlertJson(ObjectMapper mapper, String json) {
        try {
            WeatherStatusMessage msg = mapper.readValue(json, WeatherStatusMessage.class);
            var alert = mapper.createObjectNode();
            alert.put("station_id", msg.getStationId());
            alert.put("humidity", msg.getWeather().getHumidity());
            alert.put("status_timestamp", msg.getStatusTimestamp());
            alert.put("message", "Raining at station " + msg.getStationId());
            return mapper.writeValueAsString(alert);
        } catch (Exception e) {
            log.warn("Failed to build rain alert payload: {}", e.getMessage());
            return json;
        }
    }

    public void start() {
        streams.setUncaughtExceptionHandler(ex -> {
            log.error("Kafka Streams uncaught exception", ex);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        streams.start();
        log.info("Rain detection Kafka Streams topology started");
    }

    public void close() {
        streams.close();
    }
}
