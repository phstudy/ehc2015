package org.phstudy;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class ItemCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        //String[] words = line.split("\t");
        //String element = words.length > 2 ? words[2] : "na";
        String[] words = line.split(",");
        String element = words[5];

        context.write(new Text(element), new IntWritable(1));
    }
}
