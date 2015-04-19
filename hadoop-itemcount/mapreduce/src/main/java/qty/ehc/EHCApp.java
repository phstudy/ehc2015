package qty.ehc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.phstudy.ItemCount;
import org.phstudy.MyLongWritable;
import org.phstudy.SortCounterMapper;
import org.phstudy.SortCounterReducer;

public class EHCApp extends Configured implements Tool {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public int run(String[] args) throws Exception {

        for (int i = 0; i < args.length; i++) {
            String string = args[i];
            System.out.println("i=" + i + ", " + string);
        }

        Class mapperClass = Class.forName(args[2]);
        long startTime = System.currentTimeMillis();

        String input = args[0];
        String output = args[1];
        String tmpFile = "/tmp/" + System.currentTimeMillis();
        setConf(new Configuration());
        Configuration conf = getConf();

        int splitSize = 1024 * 1024 * 1024;
        conf.set("mapreduce.input.fileinputformat.split.minsize", "" + splitSize);
        conf.set("mapreduce.input.fileinputformat.split.maxsize", "" + splitSize);

        Job job = Job.getInstance(conf, "qty app");
        job.setJarByClass(EHCApp.class);
        job.setMapperClass(mapperClass);
        job.setCombinerClass(QtyReducer.class);
        job.setReducerClass(QtyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(tmpFile));
        job.waitForCompletion(true);
        long mediumTime = System.currentTimeMillis();

        boolean rst = false;
        if (job.isSuccessful()) {
            Job job2 = Job.getInstance(conf, "item count2");
            job2.setNumReduceTasks(1);
            job2.setJarByClass(ItemCount.class);

            job2.setMapperClass(SortCounterMapper.class);
            job2.setReducerClass(SortCounterReducer.class);

            job2.setOutputKeyClass(MyLongWritable.class);
            job2.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job2, new Path(tmpFile), TextInputFormat.class);

            FileOutputFormat.setOutputPath(job2, new Path(output));

            rst = job2.waitForCompletion(true);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("The output goes to: " + output);
        System.out.println("$ hdfs dfs -cat " + output + "/part-r-00000");
        System.out.println(endTime - startTime);
        System.out.println(endTime - mediumTime);
        System.exit(rst ? 0 : 1);
        return rst ? 0 : 1;
    }

    public static void main(String[] args) throws Throwable {
        EHCApp app = new EHCApp();
        app.run(args);
        // ProgramDriver driver = new ProgramDriver();
        // driver.addClass("qty app", EHCApp.class, "xd");
        //
        // driver.driver(new String[] { "qty app",
        // "/Users/qrtt1/test/ehc2015/hadoop-itemcount/D01.csv", "./sadf" });
    }
}
