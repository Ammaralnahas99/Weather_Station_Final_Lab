
SELECT
    station_id,
    battery_status,
    COUNT(*)                                                              AS reading_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY station_id), 2) AS pct_of_station
FROM weather_readings
GROUP BY station_id, battery_status
ORDER BY station_id, battery_status;

-- Same thing, aggregated across all stations, to sanity-check the global 30/40/30 split.
SELECT
    battery_status,
    COUNT(*)                                                AS reading_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)      AS pct_overall
FROM weather_readings
GROUP BY battery_status
ORDER BY battery_status;



SELECT
    station_id,
    MAX(sequence_number)                                              AS expected_messages,
    COUNT(*)                                                          AS received_messages,
    MAX(sequence_number) - COUNT(*)                                   AS dropped_messages,
    ROUND(100.0 * (MAX(sequence_number) - COUNT(*)) / MAX(sequence_number), 2) AS drop_rate_pct
FROM weather_readings
GROUP BY station_id
ORDER BY station_id;


-- =====================================================================
-- Bonus: Latest weather status per station (queried directly from the DB,
-- as required by section 2.3 of the assignment).
-- =====================================================================
SELECT DISTINCT ON (station_id)
    station_id,
    sequence_number,
    battery_status,
    to_timestamp(timestamp) AS reading_time,
    humidity,
    temperature,
    wind_speed
FROM weather_readings
ORDER BY station_id, timestamp DESC;


-- =====================================================================
-- Bonus: Count of raining readings (humidity > 70) observed per station,
-- cross-checking the Kafka Streams rain-detection processor.
-- =====================================================================
SELECT
    station_id,
    COUNT(*) AS raining_readings
FROM weather_readings
WHERE humidity > 70
GROUP BY station_id
ORDER BY station_id;
