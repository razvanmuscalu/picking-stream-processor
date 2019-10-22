package tech.picnic.assignment.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

import java.time.ZonedDateTime;

@Value
@Builder
public class PickResponse {

    @JsonProperty("article_name")
    private final String name;

    private final ZonedDateTime timestamp;
}
