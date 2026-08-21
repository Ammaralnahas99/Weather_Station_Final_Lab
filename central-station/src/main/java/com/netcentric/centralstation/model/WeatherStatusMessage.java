package com.netcentric.centralstation.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
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
