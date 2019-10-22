package tech.picnic.assignment.impl;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import tech.picnic.assignment.model.PickRequest;

class SourceReader {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String POISON_PILL = "stop";
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
        final BlockingQueue<String> lines = new LinkedBlockingQueue<>();
        final BlockingQueue<PickRequest> picks = new LinkedBlockingQueue<>();

        final var numThreads = 10;
        ExecutorService consumerExecutor = newFixedThreadPool(numThreads);
        final var consumerFutures = new ArrayList<Future<?>>();

        for (int i = 0; i < numThreads; i++) {
            final var consumerFuture = consumerExecutor.submit(() -> unmarshalLines(lines, picks));
            consumerFutures.add(consumerFuture);
        }

        ExecutorService producerExecutor = newSingleThreadExecutor();
        Future<?> producerFuture = producerExecutor.submit(() -> accumulateLines(source, lines));

        try {
            producerFuture.get(maxTime.getSeconds(), TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            System.out.println(format("Thread[%s]: Reached timeout with maxTime [%s]", currentThread().getName(), maxTime.toString()));
            producerFuture.cancel(true);
            System.out.println(format("Thread[%s]: SourceReader thread has been cancelled", currentThread().getName()));
        } catch (InterruptedException | ExecutionException e) {
            System.err.println(format("Thread[%s]: Something wrong happened.", currentThread().getName()));
            e.printStackTrace();
        } finally {
            producerExecutor.shutdownNow();
        }

        for (Future f: consumerFutures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        return new LinkedList<>(picks);
    }

    private void unmarshalLines(BlockingQueue<String> lines, BlockingQueue<PickRequest> picks) {
        try {
            while (true) {
                var line = lines.take();
                if (line.equals(POISON_PILL)) {
                    lines.put(POISON_PILL);
                    return;
                }
                toPickRequest(line).ifPresent(e -> {
                    try {
                        picks.put(e);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                });
            }
        } catch (InterruptedException e) {
            System.out.println("error in unmarshal");
        }
    }

    private void accumulateLines(InputStream source, BlockingQueue<String> lines) {
        try (final var bufferedReader = new BufferedReader(new InputStreamReader(source, UTF_8))) {
            var counter = 0;
            while (counter < maxEvents) {
                // consumes unnecessary CPU when there is a pause in input
                while (!bufferedReader.ready()) sleep(100);

                final String line = bufferedReader.readLine();
                if (line != null && !line.equals(EMPTY_LINE)) {
                    lines.put(line);
                    counter++;
                }
            }
        } catch (IOException e) {
            System.err.println(format("Thread[%s]: Something wrong happened while reading from BufferedReader.", currentThread().getName()));
            e.printStackTrace();
        } catch (InterruptedException e) {
            System.out.println(format("Thread[%s]: InputStream has been closed as application reached timeout.", currentThread().getName()));
        } finally {
            try {
                lines.put(POISON_PILL);
            } catch (InterruptedException ex) {
                System.out.println("BAAAAAD");
            }
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
