package tech.picnic.assignment.impl;

import tech.picnic.assignment.model.PickRequest;

import java.io.InputStream;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.toUnmodifiableList;
import static java.util.stream.IntStream.range;
import static tech.picnic.assignment.impl.Utilities.POISON_PILL;

class SourceReader {

    private final int maxEvents;
    private final Duration maxTime;

    SourceReader(int maxEvents, Duration maxTime) {
        this.maxEvents = maxEvents;
        this.maxTime = maxTime;
    }

    List<PickRequest> readLines(InputStream source) {
        final var lines = new LinkedBlockingQueue<String>();
        final var picks = new LinkedBlockingQueue<PickRequest>();

        runSourceReaderThread(source, lines);
        runLineUnmarshallerThreads(lines, picks);

        return new LinkedList<>(picks);
    }

    private void runSourceReaderThread(InputStream source, LinkedBlockingQueue<String> lines) {
        final var sourceReaderExecutor = newSingleThreadExecutor();
        try {
            runAsync(new SourceReaderThread(maxEvents, source, lines), sourceReaderExecutor).get(maxTime.getSeconds(), TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            System.out.println(format("Thread[%s]: Reached timeout with maxTime [%s]", currentThread().getName(), maxTime.toString()));
            System.out.println(format("Thread[%s]: SourceReader thread has been cancelled", currentThread().getName()));
        } catch (InterruptedException | ExecutionException e) {
            System.err.println(format("Thread[%s]: Something wrong happened.", currentThread().getName()));
            e.printStackTrace();
        } finally {
            sourceReaderExecutor.shutdown();
            try {
                lines.put(POISON_PILL);
            } catch (InterruptedException ex) {
                System.err.println(format("Thread[%s]: Something wrong happened while sending poison pill.", currentThread().getName()));
                ex.printStackTrace();
            }
        }
    }

    private void runLineUnmarshallerThreads(LinkedBlockingQueue<String> lines, LinkedBlockingQueue<PickRequest> picks) {
        final var numThreads = getRuntime().availableProcessors();
        final var linesUnmarshallerExecutor = newFixedThreadPool(numThreads);

        range(0, numThreads)
                .mapToObj(i -> runAsync(new LinesUnmarshallerThread(lines, picks), linesUnmarshallerExecutor))
                .collect(toUnmodifiableList()) // this terminal step is important as it starts all threads before attempting to call get() on them
                .forEach(getFuture());

        linesUnmarshallerExecutor.shutdown();
    }

    private Consumer<CompletableFuture<Void>> getFuture() {
        return f -> {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                System.err.println(format("Thread[%s]: Something wrong happened.", currentThread().getName()));
                e.printStackTrace();
            } finally {
                f.cancel(true);
            }
        };
    }

}
