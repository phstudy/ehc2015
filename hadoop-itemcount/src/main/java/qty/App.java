package qty;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ProgramDriver;
import org.apache.hadoop.util.Tool;

public class App extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // if (args.length != 2) {
        // System.err.println("Usage: wordmedian <in> <out>");
        // return 0;
        // }

        args = new String[] { "/Users/qrtt1/test/ehc2015/hadoop-itemcount/D01.csv", "./sadf_"+System.currentTimeMillis() };
        setConf(new Configuration());
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "qty app");
        job.setJarByClass(App.class);
        job.setMapperClass(QtyMapper.class);
        job.setCombinerClass(QtyReducer.class);
        job.setReducerClass(QtyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Throwable {
        App app = new App();
        app.run(null);
//        ProgramDriver driver = new ProgramDriver();
//        driver.addClass("qty app", App.class, "xd");
//
//        driver.driver(new String[] { "qty app", "/Users/qrtt1/test/ehc2015/hadoop-itemcount/D01.csv", "./sadf" });
    }
}
