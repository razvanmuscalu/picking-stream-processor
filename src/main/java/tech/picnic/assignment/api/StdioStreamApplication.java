package tech.picnic.assignment.api;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * A command line application which demonstrates how a {@link StreamProcessor} may be used.
 *
 * <p><strong>Important:</strong> do not touch this class! Don't move, rename or otherwise modify
 * it.
 */
public final class StdioStreamApplication {
    private StdioStreamApplication() {}

    /**
     * Pipes stdin through the first {@link StreamProcessor} that can {@link ServiceLoader
     * service-loaded}, sending the result to stdout.
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.printf("Usage:%n  java -jar application.jar <maxEvents> <maxTime>%n");
            System.exit(1);
        }

        int maxEvents = Integer.parseInt(args[0]);
        Duration maxTime = Duration.parse(args[1]);

        processStdio(maxEvents, maxTime);
    }

    private static void processStdio(int maxEvents, Duration maxTime) throws IOException {
        Iterator<EventProcessorFactory> factories =
                ServiceLoader.load(EventProcessorFactory.class).iterator();
        if (!factories.hasNext()) {
            throw new IllegalStateException("No EventProcessorFactory found");
        }

        try (EventProcessorFactory factory = factories.next();
                StreamProcessor processor = factory.createProcessor(maxEvents, maxTime)) {
            processor.process(System.in, System.out);
        }
    }
}
