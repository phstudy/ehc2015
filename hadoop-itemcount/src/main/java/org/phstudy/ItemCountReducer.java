package org.phstudy;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class ItemCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key,
            Iterable<IntWritable> values,
            Context context)
            throws IOException, InterruptedException
    {
        int count = 0;
        for(IntWritable value : values) {
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }    
}
