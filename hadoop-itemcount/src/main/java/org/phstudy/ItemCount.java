package org.phstudy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by study on 3/28/15.
 */
public class ItemCount {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String in = "excite-small.log";
        String out = "out/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        if (otherArgs.length >= 1) {
            in = otherArgs[0];
        }
        if (otherArgs.length >= 2) {
            out = otherArgs[1];
        }

        Job job = Job.getInstance(conf, "item count");
        job.setJarByClass(ItemCount.class);

        job.setMapperClass(ItemCountMapper.class);
        job.setCombinerClass(ItemCountReducer.class);
        job.setReducerClass(ItemCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        System.out.println("The output goes to: " + out);

        job.waitForCompletion(true);

        if(job.isSuccessful()) {
            String out2 = "out/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

            Job job2 = Job.getInstance(conf, "item count2");
            job2.setJarByClass(ItemCount.class);

            job2.setMapperClass(SortCounterMapper.class);
            job2.setReducerClass(SortCounterReducer.class);

            job2.setOutputKeyClass(MyLongWritable.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(out + "/part-r-00000"));
            FileOutputFormat.setOutputPath(job2, new Path(out2));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
