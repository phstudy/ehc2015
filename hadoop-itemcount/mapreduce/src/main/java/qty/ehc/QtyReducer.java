package qty.ehc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QtyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    protected void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException,
            InterruptedException {
        long sum = 0;
        for (LongWritable v : value) {
            sum += v.get();
        }
        context.write(key, new LongWritable(sum));
    }
}
