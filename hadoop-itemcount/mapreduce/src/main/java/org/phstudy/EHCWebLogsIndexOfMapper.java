package org.phstudy;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EHCWebLogsIndexOfMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    public final static String EXPECTED_ACT = ";act=order;";
    public final static String ORDER_LIST_START = "plist=";
    public final static String ORDER_LIST_END = ";";
    public final static String EMPTY_ORDER_LIST = ";plist=;";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String input = value.toString();

        int idx;

        /* 空的 plist skip */
        idx = input.indexOf(";plist=");
        if (idx >= 0) {
            idx += ";plist=".length();
            if (input.charAt(idx) == ';') {
                return;
            }
        }

        /* 不是 order act */
        idx = input.indexOf(";act=");
        if (idx >= 0) {
            idx += ";act=".length();
            if (input.charAt(idx) != 'o') {
                return;
            }
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

                context.write(new Text(data[x - 1]),
                        new LongWritable(Integer.parseInt(data[x]) * Integer.parseInt(data[x + 1])));

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

}
