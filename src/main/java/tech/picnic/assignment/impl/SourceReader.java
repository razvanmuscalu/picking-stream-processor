package tech.picnic.assignment.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import tech.picnic.assignment.model.PickRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

class SourceReader {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String EMPTY_LINE = "";

    private final int maxEvents;
    private final Duration maxTime;

    SourceReader(int maxEvents, Duration maxTime) {
        this.maxEvents = maxEvents;
        this.maxTime = maxTime;

        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.disable(WRITE_DATES_AS_TIMESTAMPS);
    }

    List<PickRequest> readLines(InputStream source) {
        final var lines = new LinkedList<PickRequest>();

        ExecutorService executor = newSingleThreadExecutor();
        Future<?> future = executor.submit(() -> accumulateLines(source, lines));

        try {
            future.get(maxTime.getSeconds(), TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            System.out.println(format("Thread[%s]: Reached timeout with maxTime [%s]", currentThread().getName(), maxTime.toString()));
            future.cancel(true);
            System.out.println(format("Thread[%s]: SourceReader thread has been cancelled", currentThread().getName()));
        } catch (InterruptedException | ExecutionException e) {
            System.err.println(format("Thread[%s]: Something wrong happened.", currentThread().getName()));
            e.printStackTrace();
        } finally {
            executor.shutdownNow();
        }

        return lines;
    }

    private void accumulateLines(InputStream source, List<PickRequest> lines) {
        try (final var bufferedReader = new BufferedReader(new InputStreamReader(source, UTF_8))) {
            var counter = 0;
            while (counter < maxEvents) {
                // consumes unnecessary CPU when there is a pause in input
                while (!bufferedReader.ready()) sleep(100);

                final String line = bufferedReader.readLine();
                if (line != null && !line.equals(EMPTY_LINE)) {

                    var pick = toPickRequest(line);
                    pick.ifPresent(lines::add);
                    counter++;
                }
            }
        } catch (IOException e) {
            System.err.println(format("Thread[%s]: Something wrong happened while reading from BufferedReader.", currentThread().getName()));
            e.printStackTrace();
        } catch (InterruptedException e) {
            System.out.println(format("Thread[%s]: InputStream has been closed as application reached timeout.", currentThread().getName()));
        }
    }

    private Optional<PickRequest> toPickRequest(String line) {
        try {
            return Optional.of(OBJECT_MAPPER.readValue(line, PickRequest.class));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

}
