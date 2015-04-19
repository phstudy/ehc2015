package org.phstudy;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EHCWebLogsCachedPidPricedMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    public final static String EXPECTED_ACT = ";act=order;";
    public final static String ORDER_LIST_START = "plist=";
    public final static String ORDER_LIST_END = ";";
    public final static String EMPTY_ORDER_LIST = ";plist=;";

    private static HashMap<String, Integer> pidAndPriceMap = new HashMap<String, Integer>();
    private static HashMap<String, Text> pidTextMap = new HashMap<String, Text>();
    private static HashMap<String, Integer> countStrInteger = new HashMap<String, Integer>();

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

        String originData = input.substring(pStart, eStart);
        String[] data = originData.split(",");
        if (data.length % 3 != 0) {
            System.err.println("ERR " + input);
        }

        for (int x = 1; x < data.length; x += 3) {
            try {

                String pid = data[x - 1];
                String nums = data[x];
                Integer numsInt = countStrInteger.get(nums);
                if (numsInt == null) {
                    numsInt = Integer.parseInt(nums);
                    countStrInteger.put(nums, numsInt);
                }

                if (pidAndPriceMap.containsKey(pid)) {
                    context.write(pidTextMap.get(pid), new LongWritable(numsInt * pidAndPriceMap.get(pid)));
                } else {
                    Integer price = Integer.parseInt(data[x + 1]);
                    context.write(new Text(pid), new LongWritable(numsInt * price));
                    pidAndPriceMap.put(pid, price);
                    pidTextMap.put(pid, new Text(pid));
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

}
