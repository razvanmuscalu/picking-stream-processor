package tech.picnic.assignment.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import tech.picnic.assignment.model.PickRequest;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;

public class LinesUnmarshallerThread implements Runnable {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String POISON_PILL = "stop";

    private final LinkedBlockingQueue<String> lines;
    private final LinkedBlockingQueue<PickRequest> picks;

    LinesUnmarshallerThread(LinkedBlockingQueue<String> lines, LinkedBlockingQueue<PickRequest> picks) {
        this.lines = lines;
        this.picks = picks;

        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.disable(WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public void run() {
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
                        System.err.println(format("Thread[%s]: Something wrong happened putting pick.", currentThread().getName()));
                        ex.printStackTrace();
                    }
                });
            }
        } catch (InterruptedException e) {
            System.err.println(format("Thread[%s]: Something wrong happened receiving line.", currentThread().getName()));
            e.printStackTrace();
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
