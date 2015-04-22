package org.ehc.inputv4;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import util.OrderPlistFinderWithHDFSWriter;

/**
 * A class that provides a line reader from an input stream. Depending on the
 * constructor used, lines will either be terminated by:
 * <ul>
 * <li>one of the following: '\n' (LF) , '\r' (CR), or '\r\n' (CR+LF).</li>
 * <li><em>or</em>, a custom byte sequence delimiter</li>
 * </ul>
 * In both cases, EOF also terminates an otherwise unterminated line.
 */
@InterfaceAudience.LimitedPrivate({ "MapReduce" })
@InterfaceStability.Unstable
public class MyLineReader {
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    private int bufferSize = DEFAULT_BUFFER_SIZE;
    private InputStream in;
    private byte[] buffer;
    // the number of bytes of real data in the buffer
    private int bufferLength = 0;
    // the current position in the buffer
    private int bufferPosn = 0;

    private static final byte CR = '\r';
    private static final byte LF = '\n';

    // The line delimiter
    private final byte[] recordDelimiterBytes;
    private OrderPlistFinderWithHDFSWriter orderPlistFinderWithHDFSWriter;

    /**
     * Create a line reader that reads from the given stream using the default
     * buffer-size (64k).
     * 
     * @param in
     *            The input stream
     * @throws IOException
     */
    public MyLineReader(InputStream in) {
        this(in, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Create a line reader that reads from the given stream using the given
     * buffer-size.
     * 
     * @param in
     *            The input stream
     * @param bufferSize
     *            Size of the read buffer
     * @throws IOException
     */
    public MyLineReader(InputStream in, int bufferSize) {
        this.in = in;
        this.orderPlistFinderWithHDFSWriter = new OrderPlistFinderWithHDFSWriter(in);
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = null;
    }

    /**
     * Create a line reader that reads from the given stream using the
     * <code>io.file.buffer.size</code> specified in the given
     * <code>Configuration</code>.
     * 
     * @param in
     *            input stream
     * @param conf
     *            configuration
     * @throws IOException
     */
    public MyLineReader(InputStream in, Configuration conf) throws IOException {
        this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
    }

    /**
     * Create a line reader that reads from the given stream using the default
     * buffer-size, and using a custom delimiter of array of bytes.
     * 
     * @param in
     *            The input stream
     * @param recordDelimiterBytes
     *            The delimiter
     */
    public MyLineReader(InputStream in, byte[] recordDelimiterBytes) {
        this.in = in;
        this.orderPlistFinderWithHDFSWriter = new OrderPlistFinderWithHDFSWriter(in);
        this.bufferSize = DEFAULT_BUFFER_SIZE;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
    }

    /**
     * Create a line reader that reads from the given stream using the given
     * buffer-size, and using a custom delimiter of array of bytes.
     * 
     * @param in
     *            The input stream
     * @param bufferSize
     *            Size of the read buffer
     * @param recordDelimiterBytes
     *            The delimiter
     * @throws IOException
     */
    public MyLineReader(InputStream in, int bufferSize, byte[] recordDelimiterBytes) {
        this.in = in;
        this.orderPlistFinderWithHDFSWriter = new OrderPlistFinderWithHDFSWriter(in);
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
    }

    /**
     * Create a line reader that reads from the given stream using the
     * <code>io.file.buffer.size</code> specified in the given
     * <code>Configuration</code>, and using a custom delimiter of array of
     * bytes.
     * 
     * @param in
     *            input stream
     * @param conf
     *            configuration
     * @param recordDelimiterBytes
     *            The delimiter
     * @throws IOException
     */
    public MyLineReader(InputStream in, Configuration conf, byte[] recordDelimiterBytes) throws IOException {
        this.in = in;
        this.orderPlistFinderWithHDFSWriter = new OrderPlistFinderWithHDFSWriter(in);
        this.bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
    }

    /**
     * Close the underlying stream.
     * 
     * @throws IOException
     */
    public void close() throws IOException {
        in.close();
    }

    /**
     * Read one line from the InputStream into the given Text.
     *
     * @param str
     *            the object to store the given line (without newline)
     * @param maxLineLength
     *            the maximum number of bytes to store into str; the rest of the
     *            line is silently discarded.
     * @param maxBytesToConsume
     *            the maximum number of bytes to consume in this call. This is
     *            only a hint, because if the line cross this threshold, we
     *            allow it to happen. It can overshoot potentially by as much as
     *            one buffer length.
     *
     * @return the number of bytes read including the (longest) newline found.
     *
     * @throws IOException
     *             if the underlying stream throws
     */
    public int readLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
        return readDefaultLine(str, maxLineLength, maxBytesToConsume);
    }

    byte[] bufferForOrderPlist = new byte[1024 * 128];

    /**
     * Read a line terminated by one of CR, LF, or CRLF.
     */
    private int readDefaultLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {

        int countDown = 100;
        while (!orderPlistFinderWithHDFSWriter.isEOF && countDown-- > 0) {
            orderPlistFinderWithHDFSWriter.sink();
        }

        int size = orderPlistFinderWithHDFSWriter.flushResult(bufferForOrderPlist);
        str.clear();
        if(size <=0){
            return 0;
        }
        byte[] header = ";act=order;plist=".getBytes();
        str.append(header, 0, header.length);
        str.append(bufferForOrderPlist, 0, size);
        str.append(";".getBytes(), 0, 1);
        
        return size;
    }

    /**
     * Read from the InputStream into the given Text.
     * 
     * @param str
     *            the object to store the given line
     * @param maxLineLength
     *            the maximum number of bytes to store into str.
     * @return the number of bytes read including the newline
     * @throws IOException
     *             if the underlying stream throws
     */
    public int readLine(Text str, int maxLineLength) throws IOException {
        return readLine(str, maxLineLength, Integer.MAX_VALUE);
    }

    /**
     * Read from the InputStream into the given Text.
     * 
     * @param str
     *            the object to store the given line
     * @return the number of bytes read including the newline
     * @throws IOException
     *             if the underlying stream throws
     */
    public int readLine(Text str) throws IOException {
        return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

}
