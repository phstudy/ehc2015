package org.phstudy.job;

import com.google.common.primitives.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;

/**
 * Created by study on 4/20/15.
 */
public class CopyFileDirect implements Runnable {
    private Configuration conf;
    private String in;
    private String hdfs_out_extracted;

    public CopyFileDirect(Configuration conf, String in, String hdfs_out_extracted) {
        this.conf = conf;
        this.in = in;
        this.hdfs_out_extracted = hdfs_out_extracted;
    }

    @Override
    public void run() {
        try {
            FileSystem hdfs = FileSystem.get(conf);

            Path hdfsOutExtractedFilePath = new Path(hdfs_out_extracted);

            GZIPInputStream zgis = new GZIPInputStream(new FileInputStream(in), 65536);
            byte[] outbuf = new byte[65536];

            FSDataOutputStream fsdos = hdfs.create(hdfsOutExtractedFilePath);

            int len = zgis.read(outbuf, 0, 65536);
            fsdos.write(outbuf, 512, len - 512); // skip tar header

            while ((len = zgis.read(outbuf, 0, 65536)) != -1) {
                if (outbuf[len - 1] == 0x00) {
                    break;
                }
                fsdos.write(outbuf, 0, len);
            }
            fsdos.write(outbuf, 0, Bytes.indexOf(outbuf, (byte) 0x00));

            zgis.close();
            fsdos.close();
            hdfs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}