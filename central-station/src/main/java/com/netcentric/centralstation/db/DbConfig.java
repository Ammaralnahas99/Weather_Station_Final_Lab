package com.netcentric.centralstation.db;

public class DbConfig {

    public final String url;
    public final String user;
    public final String password;

    private DbConfig(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public static DbConfig fromEnv() {
        String host = getEnv("DB_HOST", "localhost");
        String port = getEnv("DB_PORT", "5432");
        String name = getEnv("DB_NAME", "weather");
        String user = getEnv("DB_USER", "weather");
        String password = getEnv("DB_PASSWORD", "weather");
        String sslMode = getEnv("DB_SSLMODE", "prefer");

        String url = String.format("jdbc:postgresql://%s:%s/%s?sslmode=%s", host, port, name, sslMode);
        return new DbConfig(url, user, password);
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }
}
