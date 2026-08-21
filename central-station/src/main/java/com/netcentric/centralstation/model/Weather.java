package com.netcentric.centralstation.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Weather {

    @JsonProperty("humidity")
    private int humidity;

    @JsonProperty("temperature")
    private int temperature;

    @JsonProperty("wind_speed")
    private int windSpeed;

    public int getHumidity() {
        return humidity;
    }

    public int getTemperature() {
        return temperature;
    }

    public int getWindSpeed() {
        return windSpeed;
    }
}
