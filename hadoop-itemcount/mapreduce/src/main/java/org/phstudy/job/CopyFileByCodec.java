package org.phstudy.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by study on 4/20/15.
 */
public class CopyFileByCodec implements Runnable {
    private Configuration conf;
    private String in;
    private String hdfs_out;
    private String hdfs_out_extracted;

    public CopyFileByCodec(Configuration conf, String in, String hdfs_out, String hdfs_out_extracted) {
        this.conf = conf;
        this.in = in;
        this.hdfs_out = hdfs_out;
        this.hdfs_out_extracted = hdfs_out_extracted;
    }

    @Override
    public void run() {
        try {
            FileSystem hdfs = FileSystem.get(conf);

            Path localFilePath = new Path(in);
            Path hdfsOutFilePath = new Path(hdfs_out);
            Path hdfsOutExtractedFilePath = new Path(hdfs_out_extracted);

            hdfs.copyFromLocalFile(localFilePath, hdfsOutFilePath);

            CompressionCodecFactory factory = new CompressionCodecFactory(conf);
            CompressionCodec codec = factory.getCodecByClassName(GzipCodec.class.getCanonicalName());

            InputStream is = codec.createInputStream(hdfs.open(hdfsOutFilePath));
            OutputStream out = hdfs.create(hdfsOutExtractedFilePath);
            is.skip(512); //skip tar header
            IOUtils.copyBytes(is, out, conf);

            hdfs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}