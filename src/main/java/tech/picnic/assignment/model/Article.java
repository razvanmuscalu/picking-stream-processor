package tech.picnic.assignment.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

@Value
public class Article {

    private final String id;
    private final String name;

    @JsonProperty("temperature_zone")
    private final TemperatureZone temperatureZone;
}
