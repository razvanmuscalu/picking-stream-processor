package tech.picnic.assignment.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SourceReaderThread implements Runnable {

    private static final String EMPTY_LINE = "";

    private final int maxEvents;
    private final InputStream source;
    private final LinkedBlockingQueue<String> lines;

    SourceReaderThread(int maxEvents, InputStream source, LinkedBlockingQueue<String> lines) {
        this.maxEvents = maxEvents;
        this.source = source;
        this.lines = lines;
    }

    @Override
    public void run() {
        try (final var bufferedReader = new BufferedReader(new InputStreamReader(source, UTF_8))) {
            var counter = 0;
            while (counter < maxEvents) {
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
        }
    }

}
