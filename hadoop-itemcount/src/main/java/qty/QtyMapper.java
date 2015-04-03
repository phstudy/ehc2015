package qty;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class QtyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // index 5: item
        // index 6: num
        String[] token = value.toString().split(",", 0);

        try {
            String item = token[5].trim();
            String num = token[6].trim();
            Integer.parseInt(item);
            context.write(new Text(item), new IntWritable(Integer.parseInt(num)));
        } catch (Exception ignored) {
        }

    }

}
