package tech.picnic.assignment.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import tech.picnic.assignment.api.StreamProcessor;
import tech.picnic.assignment.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static java.util.Comparator.comparing;
import static java.util.Map.Entry.comparingByKey;
import static java.util.stream.Collectors.*;
import static tech.picnic.assignment.model.TemperatureZone.Ambient;

public class PickingStreamProcessor implements StreamProcessor {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TemperatureZone REQUIRED_TEMPERATURE_ZONE = Ambient;

    private final SourceReader sourceReader;

    PickingStreamProcessor(SourceReader sourceReader) {
        this.sourceReader = sourceReader;

        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.disable(WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void process(InputStream source, OutputStream sink) throws IOException {
        final var pickers = sourceReader.readLines(source)
                .stream()
                .map(toPickRequest())
                .flatMap(Optional::stream)
                .filter(requiredArticles())
                .collect(groupingBy(PickRequest::getPicker))
                .entrySet()
                .stream()
                .sorted(comparingByKey(comparing(Picker::getActiveSince).thenComparing(Picker::getId)))
                .map(toPickerWithPicks())
                .collect(toUnmodifiableList());

        OBJECT_MAPPER.writeValue(sink, pickers);
    }

    private Predicate<PickRequest> requiredArticles() {
        return pick -> pick.getArticle().getTemperatureZone() == REQUIRED_TEMPERATURE_ZONE;
    }

    private Function<Map.Entry<Picker, List<PickRequest>>, PickerWithPicks> toPickerWithPicks() {
        return entry -> {
            final var picks = entry
                    .getValue()
                    .stream()
                    .sorted(comparing(PickRequest::getTimestamp))
                    .map(toPickResponse())
                    .collect(toUnmodifiableList());
            return PickerWithPicks.builder()
                    .name(entry.getKey().getName())
                    .activeSince(entry.getKey().getActiveSince())
                    .picks(picks)
                    .build();
        };
    }

    private Function<PickRequest, PickResponse> toPickResponse() {
        return pick -> PickResponse.builder()
                .name(pick.getArticle().getName().toUpperCase())
                .timestamp(pick.getTimestamp())
                .build();
    }

    private Function<String, Optional<PickRequest>> toPickRequest() {
        return line -> {
            try {
                return Optional.of(OBJECT_MAPPER.readValue(line, PickRequest.class));
            } catch (IOException e) {
                return Optional.empty();
            }
        };
    }
}
