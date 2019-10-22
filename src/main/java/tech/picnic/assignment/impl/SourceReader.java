package tech.picnic.assignment.impl;

import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import tech.picnic.assignment.model.PickRequest;

class SourceReader {

    private static final String POISON_PILL = "stop";

    private final int maxEvents;
    private final Duration maxTime;

    SourceReader(int maxEvents, Duration maxTime) {
        this.maxEvents = maxEvents;
        this.maxTime = maxTime;
    }

    List<PickRequest> readLines(InputStream source) {
        final var lines = new LinkedBlockingQueue<String>();
        final var picks = new LinkedBlockingQueue<PickRequest>();

        final var numThreads = getRuntime().availableProcessors();
        final var linesUnmarshallerExecutor = newFixedThreadPool(numThreads);
        final var linesUnmarshallerFutures = new ArrayList<Future<?>>();

        for (int i = 0; i < numThreads; i++) {
            final var linesUnmarshallerFuture = linesUnmarshallerExecutor.submit(new LinesUnmarshallerThread(lines, picks));
            linesUnmarshallerFutures.add(linesUnmarshallerFuture);
        }

        final var sourceReaderExecutor = newSingleThreadExecutor();
        final var sourceReaderFuture = sourceReaderExecutor.submit(new SourceReaderThread(maxEvents, source, lines));

        try {
            sourceReaderFuture.get(maxTime.getSeconds(), TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            System.out.println(format("Thread[%s]: Reached timeout with maxTime [%s]", currentThread().getName(), maxTime.toString()));
            sourceReaderFuture.cancel(true);
            System.out.println(format("Thread[%s]: SourceReader thread has been cancelled", currentThread().getName()));
        } catch (InterruptedException | ExecutionException e) {
            System.err.println(format("Thread[%s]: Something wrong happened.", currentThread().getName()));
            e.printStackTrace();
        } finally {
            sourceReaderExecutor.shutdownNow();
            try {
                lines.put(POISON_PILL);
            } catch (InterruptedException ex) {
                System.err.println(format("Thread[%s]: Something wrong happened while sending poison pill.", currentThread().getName()));
                ex.printStackTrace();
            }
        }

        for (Future f: linesUnmarshallerFutures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                System.err.println(format("Thread[%s]: Something wrong happened.", currentThread().getName()));
                e.printStackTrace();
            } finally {
                f.cancel(true);
            }
        }

        linesUnmarshallerExecutor.shutdownNow();

        return new LinkedList<>(picks);
    }

}
