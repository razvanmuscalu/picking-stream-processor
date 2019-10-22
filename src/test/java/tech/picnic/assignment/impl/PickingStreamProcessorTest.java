package tech.picnic.assignment.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tech.picnic.assignment.model.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.time.ZonedDateTime.parse;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Stream.generate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static tech.picnic.assignment.model.TemperatureZone.Ambient;
import static tech.picnic.assignment.model.TemperatureZone.Chilled;


@ExtendWith(MockitoExtension.class)
class PickingStreamProcessorTest {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Picker john = new Picker("1", "john", parse("2018-02-01T00:00:00Z"));
    private static final Picker jane = new Picker("2", "jane", parse("2018-02-01T00:00:00Z"));
    private static final Picker dave = new Picker("3", "dave", parse("2018-02-01T00:00:00Z"));
    private static final Picker burt = new Picker("4", "burt", parse("2018-01-01T00:00:00Z"));

    private static final Article noodle = new Article("1", "noodle", Ambient);
    private static final Article sprite = new Article("2", "sprite", Ambient);
    private static final Article cheese = new Article("3", "cheese", Chilled);
    private static final Article potato = new Article("3", "potato", Ambient);

    private static final TypeReference<List<PickerWithPicks>> pickerWithPicksTypeReference = new TypeReference<>() {
    };

    private static PickRequest pick() {
        return new PickRequest("123", parse("2019-06-01T01:00:00Z"), john, noodle, 2);
    }

    @Mock
    private SourceReader sourceReader;

    @InjectMocks
    private PickingStreamProcessor pickingStreamProcessor;

    PickingStreamProcessorTest() {
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Test
    @DisplayName("should filter out chilled articles")
    void testFilterChilled() throws IOException {
        var picks = List.of(
                new PickRequest("1", parse("2019-06-01T01:00:00Z"), john, noodle, 1),
                new PickRequest("2", parse("2019-06-01T02:00:00Z"), john, sprite, 1),
                new PickRequest("3", parse("2019-06-01T03:00:00Z"), john, cheese, 1),
                new PickRequest("4", parse("2019-06-01T04:00:00Z"), john, potato, 1)
        );

        var actualOutput = processStream(picks);

        assertThat(actualOutput)
                .flatExtracting(PickerWithPicks::getPicks)
                .extracting(PickResponse::getName)
                .containsExactly("NOODLE", "SPRITE", "POTATO");
    }

    @RepeatedTest(2)
    @DisplayName("should have high performance")
    void testPerformance() throws IOException {
        var picks = generate(PickingStreamProcessorTest::pick).limit(1_000_000).collect(toUnmodifiableList());

        when(sourceReader.readLines(any())).thenReturn(picks);

        for (int i = 0; i < 10; i++) {
            var before = currentTimeMillis();
            processStream();
            var after = currentTimeMillis();

            System.out.println(format("Took: %s", after - before));
        }
    }

    @Test
    @DisplayName("should group articles by picker")
    void testGrouping() throws IOException {
        var picks = List.of(
                new PickRequest("1", parse("2019-06-01T01:00:00Z"), john, noodle, 1),
                new PickRequest("2", parse("2019-06-01T02:00:00Z"), john, sprite, 1),
                new PickRequest("3", parse("2019-06-01T03:00:00Z"), jane, potato, 1)
        );

        var actualOutput = processStream(picks);

        assertThat(actualOutput).hasSize(2);
        assertThat(actualOutput.get(0)).extracting(PickerWithPicks::getName).isEqualTo("john");
        assertThat(actualOutput.get(0).getPicks()).extracting(PickResponse::getName).containsExactly("NOODLE", "SPRITE");
        assertThat(actualOutput.get(1)).extracting(PickerWithPicks::getName).isEqualTo("jane");
        assertThat(actualOutput.get(1).getPicks()).extracting(PickResponse::getName).containsExactly("POTATO");
    }

    @Test
    @DisplayName("should sort pickers")
    void testPickerSort() throws IOException {
        var picks = List.of(
                new PickRequest("1", parse("2019-06-01T01:00:00Z"), john, noodle, 1),
                new PickRequest("2", parse("2019-06-01T02:00:00Z"), dave, potato, 1),
                new PickRequest("3", parse("2019-06-01T03:00:00Z"), burt, sprite, 1)
        );

        var actualOutput = processStream(picks);

        assertThat(actualOutput)
                .flatExtracting(PickerWithPicks::getName)
                .containsExactly("burt", "john", "dave");
    }

    @Test
    @DisplayName("should sort picks")
    void testPickSort() throws IOException {
        var picks = List.of(
                new PickRequest("1", parse("2019-06-01T03:00:00Z"), john, noodle, 1),
                new PickRequest("2", parse("2019-06-01T01:00:00Z"), john, potato, 1),
                new PickRequest("3", parse("2019-06-01T02:00:00Z"), john, sprite, 1)
        );

        var actualOutput = processStream(picks);

        assertThat(actualOutput)
                .flatExtracting(PickerWithPicks::getPicks)
                .extracting(PickResponse::getName)
                .containsExactly("POTATO", "SPRITE", "NOODLE");
    }

    @Test
    @DisplayName("should capitalize pick names")
    void testCapitalizePickNames() throws IOException {
        var picks = List.of(
                new PickRequest("1", parse("2019-06-01T03:00:00Z"), john, noodle, 1)
        );

        var actualOutput = processStream(picks);

        assertThat(actualOutput)
                .flatExtracting(PickerWithPicks::getPicks)
                .extracting(PickResponse::getName)
                .containsExactly("NOODLE");
    }

    private void processStream() throws IOException {
        pickingStreamProcessor.process(System.in, new ByteArrayOutputStream());
    }

    private List<PickerWithPicks> processStream(List<PickRequest> picks) throws IOException {
        when(sourceReader.readLines(any())).thenReturn(picks);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        pickingStreamProcessor.process(System.in, outputStream);

        String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);

        return objectMapper.readValue(output, pickerWithPicksTypeReference);
    }
}
