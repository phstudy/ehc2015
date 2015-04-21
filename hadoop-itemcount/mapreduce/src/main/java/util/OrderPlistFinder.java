package util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class OrderPlistFinder {

    static Log logger = LogFactory.getLog(OrderPlistFinder.class);
    ByteBuffer processBuffer = ByteBuffer.allocateDirect(1024 * 1024);
    InputStream input;
    boolean isEOF = false;

    public OrderPlistFinder(InputStream input) {
        this.input = input;
    }

    public void foo() throws IOException {
        int readCount = 0;
        byte[] buffer = new byte[1024 * 128];
        while (processBuffer.hasRemaining()) {
            int byteToRead = Math.min(buffer.length, processBuffer.remaining());
            readCount = input.read(buffer, 0, byteToRead);
            if (readCount == -1) {
                isEOF = true;
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
            findText(processBuffer, (byte) 'p', (byte) 't', (byte) '=');
            byte[] data = extractTo(processBuffer, (byte) ';');
            System.err.println(new String(data));
        }

        /* 把剩下的資料搬到前面，下一回使用 */
        byte[] tail = new byte[processBuffer.remaining()];
        processBuffer.get(tail);
        processBuffer.clear();
        processBuffer.put(tail);

    }

    private byte[] extractTo(ByteBuffer buffer, byte stopToken) {
        final int originPosition = buffer.position();
        buffer.mark();
        int stopPosition = 0;
        for (int i = 0; i < 1024; i++) {
            if (buffer.get() == stopToken) {
                stopPosition = buffer.position();
                break;
            }
        }

        // 什麼都沒找到
        if (stopPosition == 0) {
            buffer.reset();
            return new byte[0];
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
                    logger.debug("found >>" + new String(sixBytes) + "<< current pos: " + buffer.position());
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

    public static void main(String[] args) throws IOException {
        Logger.getRootLogger().setLevel(Level.DEBUG);
        FileInputStream in = new FileInputStream(
                "/Users/qrtt1/test/ehc2015/hadoop-itemcount/mapreduce/src/test/resources/sample1.txt");
        OrderPlistFinder finder = new OrderPlistFinder(in);
        while(!finder.isEOF){
            finder.foo();
        }
    }
}
