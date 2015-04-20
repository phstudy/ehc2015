package org.phstudy.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import sun.nio.ch.ChannelInputStream;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.zip.GZIPInputStream;

/**
 * Created by study on 4/20/15.
 */
public class CopyFileByNIO implements Runnable {
    private Configuration conf;
    private String in;
    private String hdfs_out_extracted;

    public CopyFileByNIO(Configuration conf, String in, String hdfs_out_extracted) {
        this.conf = conf;
        this.in = in;
        this.hdfs_out_extracted = hdfs_out_extracted;
    }

    @Override
    public void run() {
        try {
            FileSystem hdfs = FileSystem.get(conf);

            Path hdfsOutExtractedFilePath = new Path(hdfs_out_extracted);

            FileChannel channel = new FileInputStream(in).getChannel();
            GZIPInputStream zgis = new GZIPInputStream(new ChannelInputStream(channel), 65536);
            byte[] outbuf = new byte[65536];

            FSDataOutputStream fsdos = hdfs.create(hdfsOutExtractedFilePath);
            WritableByteChannel out = Channels.newChannel(fsdos);

            // TODO: skip tar header
            int len;

            while ((len = zgis.read(outbuf, 0, 65536)) != -1) {
                out.write(ByteBuffer.wrap(outbuf, 0, len));
            }

            zgis.close();
            fsdos.close();
            out.close();
            hdfs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}