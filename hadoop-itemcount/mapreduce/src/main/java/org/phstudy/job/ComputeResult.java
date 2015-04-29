package org.phstudy.job;

import com.google.common.io.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.ehc.inputv1.MyInputFormat;
import org.ehc.inputv2.MyMergePlistInputFormat;
import org.ehc.inputv3.ByteBufferOrderPlistInputFormat;
import org.phstudy.*;

import java.io.File;

/**
 * Created by study on 4/20/15.
 */
public class ComputeResult implements Runnable {
    final Configuration conf;
    private String in;
    private String out;
    private String result;

    public ComputeResult(Configuration conf, String in, String out, String result) {
        this.conf = conf;
        this.in = in;
        this.out = out;
        this.result = result;
    }

    @Override
    public void run() {
        try {
            String immediateOut = out + "-immediate";

            Job job = Job.getInstance(conf, "extract job");
            job.setJarByClass(ItemCount.class);

            job.setInputFormatClass(TextInputFormat.class);
//            job.setInputFormatClass(MyInputFormat.class);
            job.setInputFormatClass(MyMergePlistInputFormat.class);
            job.setInputFormatClass(ByteBufferOrderPlistInputFormat.class);

            job.setMapperClass(EHCWebLogsMapper.class);
            job.setCombinerClass(ItemCountReducer.class);
            job.setReducerClass(ItemCountReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            FileInputFormat.addInputPath(job, new Path(in));
            FileOutputFormat.setOutputPath(job, new Path(immediateOut));

            job.waitForCompletion(true);

            if (job.isSuccessful()) {
                conf.set("mapreduce.output.textoutputformat.separator", ",");
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