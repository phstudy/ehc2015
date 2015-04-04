package org.phstudy;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by study on 4/1/15.
 */
public class SortCounterMapper extends Mapper<LongWritable, Text, MyLongWritable, Text>{

        @Override
        protected void map(
                LongWritable key,
                Text value,
                Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("\t");
            long count = Long.valueOf(words[1]);
            String pid = words[0];

            context.write(new MyLongWritable(count), new Text(pid));
        }


}
