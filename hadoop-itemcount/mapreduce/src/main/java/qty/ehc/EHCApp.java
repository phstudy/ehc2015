package qty.ehc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ProgramDriver;
import org.apache.hadoop.util.Tool;
import org.phstudy.ItemCount;
import org.phstudy.MyLongWritable;
import org.phstudy.SortCounterMapper;
import org.phstudy.SortCounterReducer;

public class EHCApp extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // if (args.length != 2) {
        // System.err.println("Usage: wordmedian <in> <out>");
        // return 0;
        // }
        long startTime = System.currentTimeMillis();
        args = new String[] { "abc.log", "./ehc/" + System.currentTimeMillis() };

        //
        String immediateOut = "./ehc/" + System.currentTimeMillis();
        args = new String[] { "/Users/qrtt1/Downloads/EHC_1st_round.log", immediateOut };
        setConf(new Configuration());
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "qty app");
        job.setJarByClass(EHCApp.class);
        job.setMapperClass(QtyMapper.class);
        job.setCombinerClass(QtyReducer.class);
        job.setReducerClass(QtyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        String out = "./ehc_out/" + System.currentTimeMillis();
        boolean rst = false;
        if (job.isSuccessful()) {
            Job job2 = Job.getInstance(conf, "item count2");
            job2.setNumReduceTasks(1);
            job2.setJarByClass(ItemCount.class);

            job2.setMapperClass(SortCounterMapper.class);
            job2.setReducerClass(SortCounterReducer.class);

            job2.setOutputKeyClass(MyLongWritable.class);
            job2.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job2, new Path(immediateOut), TextInputFormat.class);

            FileOutputFormat.setOutputPath(job2, new Path(out));

            rst = job2.waitForCompletion(true);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("The output goes to: " + out);
        System.out.println("$ hdfs dfs -cat " + out + "/part-r-00000");
        System.out.println(endTime - startTime);
        System.exit(rst ? 0 : 1);
        return rst ? 0 : 1;
    }

    public static void main(String[] args) throws Throwable {
        EHCApp app = new EHCApp();
        app.run(null);
        // ProgramDriver driver = new ProgramDriver();
        // driver.addClass("qty app", EHCApp.class, "xd");
        //
        // driver.driver(new String[] { "qty app",
        // "/Users/qrtt1/test/ehc2015/hadoop-itemcount/D01.csv", "./sadf" });
    }
}
