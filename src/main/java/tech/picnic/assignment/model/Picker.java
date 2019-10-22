package tech.picnic.assignment.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

import java.time.ZonedDateTime;
import java.util.Objects;

@Value
public class Picker {

    private final String id;
    private final String name;

    @JsonProperty("active_since")
    private final ZonedDateTime activeSince;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Picker picker = (Picker) o;
        return id.equals(picker.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
