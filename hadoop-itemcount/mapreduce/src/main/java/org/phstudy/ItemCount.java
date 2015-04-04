package org.phstudy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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

        // you can download the dataset from http://study-student-class.s3.amazonaws.com/NUTN%20Master%20Class/Second%20year/First%20term/Data%20Mining/Homework1/Dataset_3__SuperMarket/D01.csv
        String in = "D01.csv";
        String out = "out/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        if (otherArgs.length >= 1) {
            in = otherArgs[0];
        }
        if (otherArgs.length >= 2) {
            out = otherArgs[1];
        }
        String immediateOut = out + "-immediate";

        Job job = Job.getInstance(conf, "item count");
        job.setJarByClass(ItemCount.class);

        job.setMapperClass(ItemCountMapper.class);
        job.setCombinerClass(ItemCountReducer.class);
        job.setReducerClass(ItemCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(immediateOut));

        job.waitForCompletion(true);

        boolean rst = false;
        if(job.isSuccessful()) {
            Job job2 = Job.getInstance(conf, "item count2");
            job2.setNumReduceTasks(1);
            job2.setJarByClass(ItemCount.class);

            job2.setMapperClass(SortCounterMapper.class);
            job2.setReducerClass(SortCounterReducer.class);

            job2.setOutputKeyClass(MyLongWritable.class);
            job2.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job2, new Path(immediateOut), TextInputFormat.class);
            FileOutputFormat.setOutputPath(job2, new Path(out));

            rst = job2.waitForCompletion(true);
        }
        System.out.println("The output goes to: " + out);
        System.out.println("$ hdfs dfs -cat " + out + "/part-r-00000");
        System.exit(rst ? 0 : 1);
    }
}
