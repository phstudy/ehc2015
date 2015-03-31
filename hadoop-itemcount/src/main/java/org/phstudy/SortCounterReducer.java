package org.phstudy;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by study on 4/1/15.
 */
public class SortCounterReducer extends Reducer<MyLongWritable, Text, LongWritable, Text> {
    static final int n = 20;
    static final AtomicInteger counter = new AtomicInteger(0);
    @Override
    protected void reduce(MyLongWritable count,
                          Iterable<Text> values,
                          Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            if(counter.getAndIncrement() < n) {
                context.write(count, value);
            }
        }
    }
}
