package tech.picnic.assignment.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

import java.time.ZonedDateTime;
import java.util.List;

@Value
@Builder
public class PickerWithPicks {

    @JsonProperty("picker_name")
    private final String name;

    @JsonProperty("active_since")
    private final ZonedDateTime activeSince;

    private final List<PickResponse> picks;
}
