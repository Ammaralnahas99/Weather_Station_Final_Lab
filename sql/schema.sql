-- Weather Stations Monitoring - PostgreSQL schema
-- This is also created automatically at startup by DbWriter#ensureSchema(),
-- but is provided standalone for the report / manual setup.

CREATE TABLE IF NOT EXISTS weather_readings (
    id               BIGSERIAL PRIMARY KEY,
    station_id       BIGINT      NOT NULL,
    sequence_number  BIGINT      NOT NULL,
    battery_status   VARCHAR(10) NOT NULL CHECK (battery_status IN ('low', 'medium', 'high')),
    timestamp        BIGINT      NOT NULL,
    humidity         INT         NOT NULL,
    temperature      INT         NOT NULL,
    wind_speed       INT         NOT NULL,
    UNIQUE (station_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_weather_readings_station_ts
    ON weather_readings (station_id, timestamp DESC);
