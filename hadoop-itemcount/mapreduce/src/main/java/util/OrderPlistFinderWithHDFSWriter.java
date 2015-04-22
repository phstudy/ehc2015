package util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ehc.inputv4.pipe.DataHub;
import org.phstudy.ItemCount;

public class OrderPlistFinderWithHDFSWriter extends OrderPlistFinder {

    DataHub dataHub;

    public OrderPlistFinderWithHDFSWriter(InputStream input) {
        super(input);
        dataHub = new DataHub(input, buildHdfsWriter());
        this.input = dataHub.getRedirectInput();
    }

    private OutputStream buildHdfsWriter() {

        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://master");
            FileSystem hdfs = FileSystem.get(conf);
            Path hdfsOutExtractedFilePath = new Path(ItemCount.hdfs_out_extracted);
            return hdfs.create(hdfsOutExtractedFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

}
