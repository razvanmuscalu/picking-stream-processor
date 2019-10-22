package tech.picnic.assignment.model;

import lombok.Value;

import java.time.ZonedDateTime;

@Value
public class PickRequest {

    private final String id;
    private final ZonedDateTime timestamp;
    private final Picker picker;
    private final Article article;
    private final Integer quantity;
}
