package com.netcentric.centralstation.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netcentric.centralstation.db.DbWriter;
import com.netcentric.centralstation.model.WeatherStatusMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class WeatherReadingsConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(WeatherReadingsConsumer.class);

    private static final int BATCH_SIZE = 5000;
    private static final long FLUSH_INTERVAL_MS = 5000;

    private final String bootstrapServers;
    private final String topic;
    private final DbWriter dbWriter;
    private final ObjectMapper mapper = new ObjectMapper();
    private volatile boolean running = true;

    public WeatherReadingsConsumer(String bootstrapServers, String topic, DbWriter dbWriter) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.dbWriter = dbWriter;
    }

    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "central-station-db-writer");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");

        List<WeatherStatusMessage> batch = new ArrayList<>(BATCH_SIZE);
        long lastFlush = System.currentTimeMillis();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            log.info("Weather readings consumer subscribed to '{}'", topic);

            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        batch.add(mapper.readValue(record.value(), WeatherStatusMessage.class));
                    } catch (Exception e) {
                        log.warn("Discarding unparsable record at offset {}: {}", record.offset(), e.getMessage());
                    }
                }

                boolean sizeTriggered = batch.size() >= BATCH_SIZE;
                boolean timeTriggered = !batch.isEmpty() && (System.currentTimeMillis() - lastFlush) >= FLUSH_INTERVAL_MS;

                if (sizeTriggered || timeTriggered) {
                    flush(batch, consumer);
                    lastFlush = System.currentTimeMillis();
                }
            }

            // final flush on shutdown
            flush(batch, consumer);
        }
    }

    private void flush(List<WeatherStatusMessage> batch, KafkaConsumer<String, String> consumer) {
        if (batch.isEmpty()) {
            return;
        }
        try {
            dbWriter.insertBatch(batch);
            consumer.commitSync();
            batch.clear();
        } catch (SQLException e) {
            log.error("Failed to flush batch to DB; offsets not committed, will retry on next poll", e);
        }
    }
}
