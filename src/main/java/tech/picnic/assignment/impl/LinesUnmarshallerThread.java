package tech.picnic.assignment.impl;

import tech.picnic.assignment.model.PickRequest;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static tech.picnic.assignment.impl.Utilities.OBJECT_MAPPER;
import static tech.picnic.assignment.impl.Utilities.POISON_PILL;

public class LinesUnmarshallerThread implements Runnable {

    private final LinkedBlockingQueue<String> lines;
    private final LinkedBlockingQueue<PickRequest> picks;

    LinesUnmarshallerThread(LinkedBlockingQueue<String> lines, LinkedBlockingQueue<PickRequest> picks) {
        this.lines = lines;
        this.picks = picks;
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
