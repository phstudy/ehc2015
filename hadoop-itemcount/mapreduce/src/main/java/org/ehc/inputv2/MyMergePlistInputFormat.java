package org.ehc.inputv2;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.ehc.input.common.AbsInputFormat;
import org.ehc.input.common.AbsLineRecordReader;
import org.ehc.input.common.ILineReader;

public class MyMergePlistInputFormat extends AbsInputFormat {

    @Override
    protected RecordReader<LongWritable, Text> buildRecordReader(byte[] recordDelimiterBytes) {
        return new AbsLineRecordReader() {

            @Override
            protected ILineReader create(InputStream in, Configuration conf) throws IOException {
                return new LineReaderV2(in, conf);
            }

            @Override
            protected ILineReader create(InputStream in, Configuration conf, byte[] recordDelimiterBytes)
                    throws IOException {
                return new LineReaderV2(in, conf, recordDelimiterBytes);
            }
        };
    }

}
