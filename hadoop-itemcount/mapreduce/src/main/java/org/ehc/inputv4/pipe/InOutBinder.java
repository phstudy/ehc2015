package org.ehc.inputv4.pipe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class InOutBinder implements Runnable {

    byte[] buffer = new byte[65536];
    private InputStream in;
    private OutputStream out;

    public InOutBinder(InputStream in, OutputStream out) {
        this.in = in;
        this.out = out;
    }

    @Override
    public void run() {
        try {
            while (true) {
                int count = in.read(buffer);
                if (count == -1) {
                    break;
                }
                writeOut(buffer, count);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    protected void writeOut(byte[] buffer, int count) throws IOException {
        out.write(buffer, 0, count);
    }
}