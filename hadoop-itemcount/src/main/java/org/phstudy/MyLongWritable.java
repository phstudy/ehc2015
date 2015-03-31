package org.phstudy;

import org.apache.hadoop.io.LongWritable;

/**
 * Created by study on 4/1/15.
 */
public class MyLongWritable extends LongWritable {

    public MyLongWritable() {}

    public MyLongWritable(long value) { super(value); }

    @Override
    public int compareTo(LongWritable o) {
        return super.compareTo(o) * -1;
    }
}
