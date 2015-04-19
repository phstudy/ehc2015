package qty.ehc;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class QtyMapperWithMiniReducer extends Mapper<LongWritable, Text, Text, LongWritable> {

    public final static String EXPECTED_ACT = ";act=order;";
    public final static String ORDER_LIST_START = "plist=";
    public final static String ORDER_LIST_END = ";";
    public final static String EMPTY_ORDER_LIST = ";plist=;";
    int ccc;

    private ConcurrentHashMap<String, LongWritable> localCache = new ConcurrentHashMap<String, LongWritable>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String input = value.toString();

        if (!input.contains(EXPECTED_ACT) || input.contains(EMPTY_ORDER_LIST)) {
            return;
        }

        ccc ++;
        int pStart = input.indexOf(ORDER_LIST_START);
        if (pStart != -1) {
            pStart += ORDER_LIST_START.length();
        }
        int eStart = input.indexOf(ORDER_LIST_END, pStart);

        String originData = input.substring(pStart, eStart);
        String[] data = originData.split(",");
        if (data.length % 3 != 0) {
            System.err.println("ERR " + input);
        }

        for (int x = 1; x < data.length; x += 3) {
            try {

                /* 把結果暫存在 Mapper 內，在 clean up 時才送出去 */
                if (!localCache.containsKey(data[x - 1])) {
                    localCache.put(data[x - 1], new LongWritable(0));
                }

                LongWritable v = localCache.get(data[x - 1]);
                v.set(v.get() + Integer.parseInt(data[x]) * Integer.parseInt(data[x + 1]));

            } catch (Exception e) {
                System.err.println(x);
                System.err.println(originData);
                e.printStackTrace();
            }

        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Entry<String, LongWritable> e : localCache.entrySet()) {
            context.write(new Text(e.getKey()), e.getValue());
        }
        
        System.out.println(Thread.currentThread().getName() + " count: " + ccc);
    }

}
