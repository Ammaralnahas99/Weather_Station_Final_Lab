package com.netcentric.weatherstation.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WeatherStatusMessage {

    @JsonProperty("station_id")
    private long stationId;

    @JsonProperty("s_no")
    private long sNo;

    @JsonProperty("battery_status")
    private String batteryStatus;

    @JsonProperty("status_timestamp")
    private long statusTimestamp;

    @JsonProperty("weather")
    private Weather weather;

    public WeatherStatusMessage() {
    }

    public WeatherStatusMessage(long stationId, long sNo, String batteryStatus, long statusTimestamp, Weather weather) {
        this.stationId = stationId;
        this.sNo = sNo;
        this.batteryStatus = batteryStatus;
        this.statusTimestamp = statusTimestamp;
        this.weather = weather;
    }

    public long getStationId() {
        return stationId;
    }

    public long getsNo() {
        return sNo;
    }

    public String getBatteryStatus() {
        return batteryStatus;
    }

    public long getStatusTimestamp() {
        return statusTimestamp;
    }

    public Weather getWeather() {
        return weather;
    }
}
