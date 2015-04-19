package org.phstudy;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 * Created by study on 3/28/15.
 */
public class ItemCount {
    private static String in = "./EHC_1st.tar.gz";
    private static String out = "out/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

    private static String result = "./Team01_Result.txt";
    private static String hdfs_out = "hdfs://master/tmp/Team01/EHC_1st.tar.gz";
    private static String hdfs_out_extracted = "hdfs://master/tmp/Team01/EHC_1st_round.log";

    private static Logger logger = Logger.getLogger("ItemCount");

    public static void main(String[] args) throws Exception {
        ExecutorService es = Executors.newFixedThreadPool(8);
        //System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        //conf.set("mapred.min.split.size", "3");
        //conf.set("io.sort.mb", "256");

        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        if (remainingArgs.length != 2) {
            logger.info("Use default input and output...");
            remainingArgs = new String[]{in, out};
        }
        in = remainingArgs[0];
        out = remainingArgs[1];
        es.execute(new ComputeResult(conf)); // take 17 secs...
        //es.execute(new CopyFile()); // take 20 secs...
        es.execute(new CopyFile2()); // take 17 secs...

        es.shutdown();
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    static class CopyFile implements Runnable {
        @Override
        public void run() {
            try {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", "hdfs://master");

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

    static class CopyFile2 implements Runnable {
        @Override
        public void run() {
            try {
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", "hdfs://master");

                FileSystem hdfs = FileSystem.get(conf);

                Path hdfsOutExtractedFilePath = new Path(hdfs_out_extracted);

                GZIPInputStream zgis = new GZIPInputStream(new FileInputStream(in), 65536);
                byte[] outbuf = new byte[65536];

                FSDataOutputStream fsdos = hdfs.create(hdfsOutExtractedFilePath);

                int len = zgis.read(outbuf, 0, 65536);
                fsdos.write(outbuf, 512, len - 512); // skip tar header

                while ((len = zgis.read(outbuf, 0, 65536)) != -1) {
                    fsdos.write(outbuf, 0, len);
                }

                zgis.close();
                fsdos.close();
                hdfs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class ComputeResult implements Runnable {
        final Configuration conf;

        public ComputeResult(Configuration conf) {
            this.conf = conf;
        }

        @Override
        public void run() {
            try {
                String immediateOut = out + "-immediate";

                Job job = Job.getInstance(conf, "extract job");
                job.setJarByClass(ItemCount.class);

                job.setInputFormatClass(TextInputFormat.class);

                job.setMapperClass(EHCWebLogsMapper.class);
                job.setCombinerClass(ItemCountReducer.class);
                job.setReducerClass(ItemCountReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(LongWritable.class);

                FileInputFormat.addInputPath(job, new Path(in));
                FileOutputFormat.setOutputPath(job, new Path(immediateOut));

                job.waitForCompletion(true);

                if (job.isSuccessful()) {
                    Job job2 = Job.getInstance(conf, "sort job");
                    job2.setNumReduceTasks(1);
                    job2.setJarByClass(ItemCount.class);

                    job2.setMapperClass(SortCounterMapper.class);
                    job2.setReducerClass(SortCounterReducer.class);

                    job2.setOutputKeyClass(MyLongWritable.class);
                    job2.setOutputValueClass(Text.class);

                    MultipleInputs.addInputPath(job2, new Path(immediateOut), TextInputFormat.class);
                    FileOutputFormat.setOutputPath(job2, new Path(out));

                    job2.waitForCompletion(true);
                }

                //logger.info("The output goes to: " + out);
                //logger.info("$ cat " + out + "/part-r-00000");

                Files.copy(new File(out + "/part-r-00000"), new File(result));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
