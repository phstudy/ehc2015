package org.ehc.input.common;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public interface ILineReader {

    /**
     * Close the underlying stream.
     * @throws IOException
     */
    public void close() throws IOException;

    /**
     * Read one line from the InputStream into the given Text.
     *
     * @param str the object to store the given line (without newline)
     * @param maxLineLength the maximum number of bytes to store into str;
     *  the rest of the line is silently discarded.
     * @param maxBytesToConsume the maximum number of bytes to consume
     *  in this call.  This is only a hint, because if the line cross
     *  this threshold, we allow it to happen.  It can overshoot
     *  potentially by as much as one buffer length.
     *
     * @return the number of bytes read including the (longest) newline
     * found.
     *
     * @throws IOException if the underlying stream throws
     */
    public int readLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException;

    /**
     * Read from the InputStream into the given Text.
     * @param str the object to store the given line
     * @param maxLineLength the maximum number of bytes to store into str.
     * @return the number of bytes read including the newline
     * @throws IOException if the underlying stream throws
     */
    public int readLine(Text str, int maxLineLength) throws IOException;

    /**
     * Read from the InputStream into the given Text.
     * @param str the object to store the given line
     * @return the number of bytes read including the newline
     * @throws IOException if the underlying stream throws
     */
    public int readLine(Text str) throws IOException;

}