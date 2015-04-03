package qty;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class QtyMapperTest {

    private final static LongWritable KEY = new LongWritable(1);
    private MapDriver<LongWritable, Text, Text, IntWritable> driver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        driver = new MapDriver<LongWritable, Text, Text, IntWritable>().withMapper(new QtyMapper());
        reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>().withReducer(new QtyReducer());
    }

    @Test
    public void testMapperNoItemOutput() {

        Text value = new Text("Date-Time, ID, Age, Area, Item_class, Item, num, cost, price");
        driver.withInput(KEY, value).runTest();
        assertEquals(0, driver.getCounters().countCounters());

    }

    @Test
    public void testMapperItemCount() {

        // Date-Time, ID, Age, Area, Item_class, Item, num, cost, price
        Text value = new Text("2001-01-04 00:00:00,00477796  ,E ,H ,100108,50853991     ,2,220,270");
        driver.withInput(KEY, value).withOutput(new Text("50853991"), new IntWritable(2)).runTest();

    }

    @Test
    public void testReducer() throws Exception {

        reduceDriver.withInputKey(new Text("123"))
                .withInputValues(Arrays.asList(new IntWritable(1), new IntWritable(1), new IntWritable(1)))
                .withOutput(new Text("123"), new IntWritable(3)).runTest();
    }

}
