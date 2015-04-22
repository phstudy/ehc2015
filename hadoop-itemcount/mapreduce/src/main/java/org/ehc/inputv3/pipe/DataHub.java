package org.ehc.inputv3.pipe;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;

/**
 * [SourceReader] =PIPE=> [HDFS WRITER] =PIPE=> [Hadoop InputFormat]
 */
public class DataHub {

    private InputStream from;
    private OutputStream duplicatedOutput;

    private PipedInputStream firstPipeIn;
    private PipedOutputStream firstPipeOut;

    private PipedInputStream secondPipeIn;
    private PipedOutputStream secondPipeOut;

    public DataHub(InputStream from, OutputStream duplicatedOutput) {
        this.from = from;
        this.duplicatedOutput = duplicatedOutput;

        preparePipes();
        bindPipeline();
    }

    protected void preparePipes() {
        try {
            firstPipeIn = new PipedInputStream(65536);
            firstPipeOut = new PipedOutputStream(firstPipeIn);

            secondPipeIn = new PipedInputStream(65536);
            secondPipeOut = new PipedOutputStream(secondPipeIn);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Thread t1;
    private Thread t2;

    private void bindPipeline() {
        t1 = new Thread(new SourceReader(from, firstPipeOut));
        t2 = new Thread(new HDFSWriter(firstPipeIn, secondPipeOut, duplicatedOutput));
        t1.start();
        t2.start();
    }

    public InputStream getRedirectInput() {
        return secondPipeIn;
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
        FileOutputStream duplicated = new FileOutputStream("duplicated.txt");

        DataHub dataHub = new DataHub(from, duplicated);
        IOUtils.copy(dataHub.getRedirectInput(), System.out);
        dataHub.join();
    }
}
