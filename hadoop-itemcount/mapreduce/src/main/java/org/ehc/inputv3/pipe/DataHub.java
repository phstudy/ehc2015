package org.ehc.inputv3.pipe;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.zip.GZIPInputStream;

/**
 * [SourceReader] =PIPE=> [HDFS WRITER] =PIPE=> [Hadoop InputFormat]
 */
public class DataHub {

    private InputStream from;
    private OutputStream to;
    private OutputStream duplicatedOutput;

    private PipedInputStream firstPipeIn;
    private PipedOutputStream firstPipeOut;

    public DataHub(InputStream from, OutputStream to, OutputStream duplicatedOutput) {
        this.from = from;
        this.to = to;
        this.duplicatedOutput = duplicatedOutput;

        preparePipes();
        bindPipeline();
    }

    protected void preparePipes() {
        try {
            firstPipeIn = new PipedInputStream(65536);
            firstPipeOut = new PipedOutputStream(firstPipeIn);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Thread t1;
    private Thread t2;

    private void bindPipeline() {
        t1 = new Thread(new SourceReader(from, firstPipeOut));
        t2 = new Thread(new HDFSWriter(firstPipeIn, to, duplicatedOutput));
        t1.start();
        t2.start();
    }

    public void join() throws InterruptedException {
        t1.join();
        t2.join();
    }

    static class SourceReader extends InOutBinder {

        public SourceReader(InputStream in, OutputStream out) {
            super(in, out);
        }
    }

    static class HDFSWriter extends InOutBinder {

        private OutputStream hdfsOut;

        public HDFSWriter(InputStream in, OutputStream out, OutputStream hdfsOut) {
            super(in, out);
            this.hdfsOut = hdfsOut;
        }

        @Override
        protected void writeOut(byte[] buffer, int count) throws IOException {
            super.writeOut(buffer, count);
            hdfsOut.write(buffer, 0, count);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        // ByteArrayInputStream from = new
        // ByteArrayInputStream(";askjdf;aklsjdflakjsdfalksdjfalskdfj".getBytes());
        InputStream from = new GZIPInputStream(new FileInputStream("EHC_1st.tar.gz"));
        FileOutputStream out1 = new FileOutputStream("out1.txt");
        FileOutputStream out2 = new FileOutputStream("out2.txt");

        DataHub dataHub = new DataHub(from, out1, out2);
        dataHub.join();
    }
}
