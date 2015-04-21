package org.phstudy;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by study on 4/1/15.
 */
public class SortCounterReducer extends Reducer<MyLongWritable, Text, Text, Text> {
    static final int n = 20;
    static final AtomicInteger counter = new AtomicInteger(0);
    @Override
    protected void reduce(MyLongWritable count,
                          Iterable<Text> values,
                          Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            int cnt = counter.getAndIncrement();
            if(cnt < n) {
                context.write(new Text(String.format("%02d", cnt + 1)), value);
            }
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        while (context.nextKey()) {
            if(counter.get() < 20) {
                reduce(context.getCurrentKey(), context.getValues(), context);
            }
        }
        cleanup(context);
    }
}
