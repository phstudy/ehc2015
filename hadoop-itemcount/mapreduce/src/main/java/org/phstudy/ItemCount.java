package org.phstudy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.phstudy.job.ComputeResult;
import org.phstudy.job.CopyFileByCodec;
import org.phstudy.job.CopyFileDirect;
import org.phstudy.job.CopyFileByNIO;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Created by study on 3/28/15.
 */
public class ItemCount {
    private static String TEAM_NUMBER = "Team34";
    private static String in = "./EHC_1st.tar.gz";
    private static String out = "out/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    private static String method = "0";

    private static String result = "./" + TEAM_NUMBER + "_Result.txt";
    private static String hdfs_out = "hdfs://master/tmp/" + TEAM_NUMBER + "/EHC_1st.tar.gz";
    private static String hdfs_out_extracted = "hdfs://master/tmp/" + TEAM_NUMBER + "/EHC_1st_round.log";

    private static Logger logger = Logger.getLogger("ItemCount");

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        //System.setProperty("HADOOP_USER_NAME", "root");

        // local fs configuration
        Configuration localFsConf = new Configuration();
        //localFsConf.set("mapred.min.split.size", "3");
        //localFsConf.set("io.sort.mb", "256");

        // hdfs configuration
        Configuration hdfsConf = new Configuration();
        hdfsConf.set("fs.defaultFS", "hdfs://master");


        // parse parameters
        GenericOptionsParser optionParser = new GenericOptionsParser(localFsConf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 3) {
            logger.info("Use default input and output...");
            remainingArgs = new String[]{in, out, method};
        }
        in = remainingArgs[0];
        out = remainingArgs[1];
        method = remainingArgs[2];


        // start jobs
        ExecutorService es = Executors.newFixedThreadPool(8);
        es.execute(new ComputeResult(localFsConf, in, out, result)); // take 17 secs...

        switch(Integer.parseInt(method)) {
            case 1:
                es.execute(new CopyFileByCodec(hdfsConf, in, out, hdfs_out_extracted)); // take 20 secs...
                break;
            case 2:
                es.execute(new CopyFileDirect(hdfsConf, in, hdfs_out_extracted)); // take 17 secs...
                break;
            case 3:
                es.execute(new CopyFileByNIO(hdfsConf, in, hdfs_out_extracted)); // take 15~20 secs...
                break;
            default:
                logger.info("No Copy File Job executed.");
        }

        es.shutdown();
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        // done
        long estimatedTime = System.currentTimeMillis() - startTime;
        logger.info("Time elapsed: " + estimatedTime);
        System.exit(0);
    }
}


