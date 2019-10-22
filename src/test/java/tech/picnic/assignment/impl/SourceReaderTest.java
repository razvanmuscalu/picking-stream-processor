package tech.picnic.assignment.impl;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.lang.System.currentTimeMillis;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.Stream.generate;
import static org.assertj.core.api.Assertions.assertThat;

class SourceReaderTest {

    private static String pickLine = "{\"timestamp\":\"2018-12-20T11:50:48Z\",\"id\":\"2344\",\"picker\":{\"id\":\"14\",\"name\":\"Joris\",\"active_since\":\"2018-09-20T08:20:00Z\"}," +
            "\"article\":{\"id\":\"13473\",\"name\":\"ACME Bananas\",\"temperature_zone\":\"ambient\"},\"quantity\":2}\n";

    private static String pick() {
        return pickLine;
    }

    @Test
    @DisplayName("should process maxEvents")
    void testMaxEvents() throws IOException {
        var lines = generate(SourceReaderTest::pick).limit(200).collect(toUnmodifiableList());

        var actualOutput = readInputStream(lines, 100, ofSeconds(30));

        assertThat(actualOutput).hasSize(100);
    }

    @RepeatedTest(2)
    @DisplayName("should have high performance")
    void testPerformance() throws IOException {
        var runs = 10;
        var ignoredRuns = 2;
        var lines = generate(SourceReaderTest::pick).limit(1_000_000).collect(toUnmodifiableList());

        var total = 0;
        for (int i = 0; i < runs; i++) {
            var before = currentTimeMillis();
            var actualOutput = readInputStream(lines, 1_000_000, ofSeconds(30));
            var after = currentTimeMillis();

            assertThat(actualOutput).hasSize(1_000_000);

            System.out.println(format("Took: %s", after - before));

            // JVM warm-up; ignore first 2 runs
            if (i >= ignoredRuns) total += after - before;
        }

        int average = total / (runs - ignoredRuns);
        System.out.println(format("Took on average: %s", average));
        assertThat(average).isLessThan(1000);
    }

    @Test
    @DisplayName("should close stream after reaching maxEvents and not wait for maxTime to elapse")
    void testCloseAfterMaxEvents() throws IOException {
        var lines = List.of("line");

        var now = currentTimeMillis();

        readInputStream(lines, 1, ofSeconds(5));

        assertThat(ofMillis(currentTimeMillis() - now)).isLessThan(ofSeconds(1));
    }

    @Test
    @DisplayName("should process received events before maxTime")
    void testMaxTime() throws IOException {
        var lines = List.of("line");

        var actualOutput = readInputStream(lines, 100, ofSeconds(3));

        assertThat(actualOutput).hasSize(1);
    }

    @Test
    @DisplayName("should close stream after maxTime")
    void testCloseAfterMaxTime() throws IOException {
        var lines = List.of("line");

        var now = currentTimeMillis();

        readInputStream(lines, 100, ofSeconds(3));

        assertThat(ofMillis(currentTimeMillis() - now)).isBetween(ofSeconds(2), ofSeconds(4));
    }

    private List<String> readInputStream(List<String> lines, int maxEvents, Duration maxTime) throws IOException {
        var sourceReader = new SourceReader(maxEvents, maxTime);

        var input = join("\n", lines);

        InputStream inputStream = new ByteArrayInputStream(input.getBytes());

        return sourceReader.readLines(inputStream);
    }
}
