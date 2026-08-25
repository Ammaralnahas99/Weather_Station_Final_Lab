package com.netcentric.centralstation.db;

import com.netcentric.centralstation.model.WeatherStatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;


public class DbWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DbWriter.class);

    private static final String INSERT_SQL =
            "INSERT INTO weather_readings " +
            "(station_id, sequence_number, battery_status, timestamp, humidity, temperature, wind_speed) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (station_id, sequence_number) DO NOTHING";

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS weather_readings (" +
            "  id BIGSERIAL PRIMARY KEY," +
            "  station_id BIGINT NOT NULL," +
            "  sequence_number BIGINT NOT NULL," +
            "  battery_status VARCHAR(10) NOT NULL," +
            "  timestamp BIGINT NOT NULL," +
            "  humidity INT NOT NULL," +
            "  temperature INT NOT NULL," +
            "  wind_speed INT NOT NULL," +
            "  UNIQUE (station_id, sequence_number)" +
            ")";

    private static final String CREATE_INDEX_SQL =
            "CREATE INDEX IF NOT EXISTS idx_weather_readings_station_ts " +
            "ON weather_readings (station_id, timestamp DESC)";

    private static final int MAX_CONNECT_ATTEMPTS = 10;
    private static final long CONNECT_RETRY_DELAY_MS = 3000;

    private final Connection connection;

    public DbWriter(DbConfig config) throws SQLException {
        this.connection = connectWithRetry(config);
        this.connection.setAutoCommit(false);
        ensureSchema();
    }

   
    private static Connection connectWithRetry(DbConfig config) throws SQLException {
        SQLException lastFailure = null;
        for (int attempt = 1; attempt <= MAX_CONNECT_ATTEMPTS; attempt++) {
            try {
                return DriverManager.getConnection(config.url, config.user, config.password);
            } catch (SQLException e) {
                lastFailure = e;
                log.warn("DB connection attempt {}/{} failed: {}", attempt, MAX_CONNECT_ATTEMPTS, e.getMessage());
                try {
                    Thread.sleep(CONNECT_RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted while retrying DB connection", ie);
                }
            }
        }
        throw lastFailure;
    }

    private void ensureSchema() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(CREATE_TABLE_SQL);
            stmt.execute(CREATE_INDEX_SQL);
            connection.commit();
        }
    }

    
    public void insertBatch(List<WeatherStatusMessage> batch) throws SQLException {
        if (batch.isEmpty()) {
            return;
        }
        try (PreparedStatement ps = connection.prepareStatement(INSERT_SQL)) {
            for (WeatherStatusMessage msg : batch) {
                ps.setLong(1, msg.getStationId());
                ps.setLong(2, msg.getsNo());
                ps.setString(3, msg.getBatteryStatus());
                ps.setLong(4, msg.getStatusTimestamp());
                ps.setInt(5, msg.getWeather().getHumidity());
                ps.setInt(6, msg.getWeather().getTemperature());
                ps.setInt(7, msg.getWeather().getWindSpeed());
                ps.addBatch();
            }
            ps.executeBatch();
            connection.commit();
            log.info("Persisted batch of {} readings", batch.size());
        } catch (SQLException e) {
            connection.rollback();
            log.error("Batch insert failed, rolled back", e);
            throw e;
        }
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}
