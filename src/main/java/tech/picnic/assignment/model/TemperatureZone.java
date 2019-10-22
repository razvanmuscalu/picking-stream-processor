package tech.picnic.assignment.model;

import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum TemperatureZone {
    Ambient("ambient"),
    Chilled("chilled");

    @JsonValue
    private final String displayName;
}
