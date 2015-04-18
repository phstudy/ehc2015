package qty.ehc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class QtyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public final static String EXPECTED_ACT = ";act=order;";
    public final static String ORDER_LIST_START = "plist=";
    public final static String ORDER_LIST_END = ";";
    public final static String EMPTY_ORDER_LIST = ";plist=;";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String input = value.toString();

        if (!input.contains(EXPECTED_ACT) || input.contains(EMPTY_ORDER_LIST)) {
            return;
        }

        int pStart = input.indexOf(ORDER_LIST_START);
        if (pStart != -1) {
            pStart += ORDER_LIST_START.length();
        }
        int eStart = input.indexOf(ORDER_LIST_END, pStart);

        String[] data = input.substring(pStart, eStart).split(",");
        if (data.length % 3 != 0) {
            System.err.println("ERR " + input);
        }

        Text outKey = null;
        IntWritable outValue = null;
        int count = 0;
        for (String i : data) {
            if (outKey != null & outValue != null) {
                context.write(outKey, outValue);
                outKey = null;
                outValue = null;
                count = 0;
            }

            if (outKey == null) {
                outKey = new Text(i);
                continue;
            }
            if (count == 0 && outValue == null) {
                count = Integer.parseInt(i);
                continue;
            }
            if (outKey != null && count != 0) {
                outValue = new IntWritable(count * Integer.parseInt(i));
                continue;
            }
        }

    }

}
