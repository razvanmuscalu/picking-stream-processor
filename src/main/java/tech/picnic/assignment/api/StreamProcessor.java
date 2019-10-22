package tech.picnic.assignment.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.annotation.WillNotClose;

/**
 * A generic interface for the processing of a stream of bytes.
 *
 * <p><strong>Important:</strong> do not touch this interface! Don't move, rename or otherwise
 * modify it.
 */
public interface StreamProcessor extends AutoCloseable {
    /**
     * Processes data from the given input stream, sending the result to the provided output stream.
     *
     * @param source The source of data to be processed.
     * @param sink The sink to which the processing result is sent.
     * @throws IOException Upon failure to read from the source or write to the sink.
     */
    void process(@WillNotClose InputStream source, @WillNotClose OutputStream sink)
            throws IOException;

    /** Closes this processor, relinquishing any underlying resources. */
    @Override
    default void close() {}
}
