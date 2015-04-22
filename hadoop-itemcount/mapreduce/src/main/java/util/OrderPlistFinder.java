package util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.phstudy.ItemCount;

public class OrderPlistFinder {

    static Log logger = LogFactory.getLog(OrderPlistFinder.class);
    ByteBuffer processBuffer = ByteBuffer.allocateDirect(64 * 1024);
    ByteBuffer resultBuffer = ByteBuffer.allocateDirect(1281674);
    protected InputStream input;
    public boolean isEOF = false;

    PipedInputStream pipeIn = new PipedInputStream(1024*1024);
    PipedOutputStream pipeOut;

    public OrderPlistFinder(InputStream input) {
        this.input = input;

        try {
            pipeOut = new PipedOutputStream(pipeIn);
        } catch (IOException e) {
            e.printStackTrace();
        }

        new Thread("PipeToHDFS") {
            public void run() {

                OutputStream hdfsOut = null;
                try {
                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", "hdfs://master");
                    FileSystem hdfs = FileSystem.get(conf);
                    Path hdfsOutExtractedFilePath = new Path(ItemCount.hdfs_out_extracted);
                    hdfsOut = hdfs.create(hdfsOutExtractedFilePath);
                    byte[] buffer = new byte[1024*1024];
                    while (true) {
                        int count = pipeIn.read(buffer);
                        if (count == -1) {
                            break;
                        }
                        hdfsOut.write(buffer, 0, count);
                    }
                    logger.warn("finish hdfs pipeline");
                } catch (IOException e) {
                    e.printStackTrace();
                }finally{
                    try {
                        hdfsOut.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        }.start();

    }

    public void sink() throws IOException {
        int readCount = 0;
        byte[] buffer = new byte[64 * 1024];
        while (processBuffer.hasRemaining()) {
            int byteToRead = Math.min(buffer.length, processBuffer.remaining());
            readCount = readData(buffer, byteToRead);
            if (readCount == -1) {
                isEOF = true;
                closePipe();
                break;
            }
            processBuffer.put(buffer, 0, readCount);
        }
        processBuffer.flip();

        // ; a c t = o
        // 0 1 2 3 4 5
        while (findText(processBuffer, (byte) ';', (byte) '=', (byte) 'o')) {
            // p l i s t =
            // 0 1 2 3 4 5

            int pos = processBuffer.position();

            if (findText(processBuffer, (byte) 'p', (byte) 't', (byte) '=')) {
                byte[] data = extractTo(processBuffer, (byte) ';');

                if (data == null) {
                    processBuffer.position(pos - 6);
                    break;
                } else if (data.length == 0) {
                    break;
                }
                resultBuffer.put(data).put((byte) ',');
            } else {
                processBuffer.position(pos - 6);
                break;
            }
        }

        /* 把剩下的資料搬到前面，下一回使用 */
        byte[] tail = new byte[processBuffer.remaining()];
        processBuffer.get(tail);
        processBuffer.clear();
        processBuffer.put(tail);

    }

    private void closePipe() {
        try {
            pipeOut.flush();
            pipeOut.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected int readData(byte[] buffer, int byteToRead) throws IOException {
        int readCount;
        readCount = input.read(buffer, 0, byteToRead);
        if (readCount != -1) {
            pipeOut.write(buffer, 0, readCount);
        }
        return readCount;
    }

    private byte[] extractTo(ByteBuffer buffer, byte stopToken) {
        final int originPosition = buffer.position();
        buffer.mark();
        int stopPosition = 0;
        for (int i = 0; i < 1024; i++) {
            if (buffer.hasRemaining() && buffer.get() == stopToken) {
                stopPosition = buffer.position();
                break;
            }
        }

        // 什麼都沒找到
        if (stopPosition == 0) {
            buffer.reset();
            return null;
        }

        buffer.reset();
        int length = stopPosition - originPosition;
        if (length == 1) {
            return new byte[0];
        }
        length -= 1;
        byte[] dst = new byte[length];
        buffer.get(dst, 0, length);

        // skip stopToken
        buffer.get();
        return dst;
    }

    private boolean findText(ByteBuffer buffer, byte firstByte, byte right2, byte right1) {
        // ; a c t = o
        // 0 1 2 3 4 5
        final int sixLimit = 6;
        byte[] sixBytes = new byte[sixLimit];
        while (buffer.remaining() > sixLimit) {
            buffer.get(sixBytes);
            if (sixBytes[0] == firstByte && sixBytes[4] == right2 && sixBytes[5] == right1) {
                if (logger.isDebugEnabled()) {
                    // logger.debug("found >>" + new String(sixBytes) +
                    // "<< current pos: " + buffer.position());
                }
                return true;
            } else {
                int currentPos = buffer.position();
                for (int i = 1; i < sixLimit; i++) {
                    // ? ; ? ? ? ? <== {i=1}
                    // 0 1 2 3 4 5
                    if (sixBytes[i] == firstByte) {
                        buffer.position(currentPos - (sixLimit - i));
                        break;
                    }
                }

            }
        }

        return false;
    }

    // public boolean isResultFull() {
    // System.out.println(resultBuffer.remaining());
    // return resultBuffer.remaining() > 1024 * 16;
    // }

    public int flushResult(byte[] largeBuffer) {
        resultBuffer.flip();
        if (resultBuffer.remaining() > largeBuffer.length) {
            logger.warn("result byteArray is too small");
        }
        int consumed = Math.min(resultBuffer.remaining(), largeBuffer.length);
        boolean skipTail = false;
        if (consumed == resultBuffer.remaining()) {
            skipTail = true;
        }
        resultBuffer.get(largeBuffer, 0, consumed);
        if (resultBuffer.hasRemaining()) {
            throw new IllegalStateException("result buffer should be flush >\"<");
        }
        resultBuffer.clear();

        if (skipTail) {
            // logger.debug("skip: " + consumed);
            consumed -= 1;
        }
        return consumed;
    }

    public static void main(String[] args) throws IOException {
        Logger.getRootLogger().setLevel(Level.DEBUG);
        InputStream in = OrderPlistFinder.class.getResourceAsStream("/sample1.txt");
        OrderPlistFinder finder = new OrderPlistFinder(in);
        while (!finder.isEOF) {
            finder.sink();
        }

        byte[] data = new byte[1281674];
        int count = finder.flushResult(data);
        System.out.println(new String(data, 0, count));

        String[] d = new String(data, 0, count).split(",");
        int cnt = 0;
        for (int i = 0; i < d.length; i += 3) {
            if (d[i].equals("0005772981"))
                cnt += Integer.parseInt(d[i + 1]);
        }
        System.out.println(cnt);
        System.out.println(cnt * 699);
    }
}
