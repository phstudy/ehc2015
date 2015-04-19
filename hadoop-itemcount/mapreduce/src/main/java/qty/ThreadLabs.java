package qty;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ThreadLabs {

    public static void main(String[] args) throws InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(8);
        es.execute(newJob());
        es.execute(newJob());
        System.out.println("invoke es shutdown");
        es.shutdown();

        System.out.println("wait for jobs");
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        System.out.println("all done");
    }

    protected static Runnable newJob() {
        return new Runnable() {

            int count = 10;
            Random rnd = new Random();

            @Override
            public void run() {

                while (count-- > 0) {
                    long t = Math.abs(rnd.nextLong() % 10000);
                    try {
                        System.out.println(Thread.currentThread().getName() + " sleep " + t + " count: " + count);
                        Thread.sleep(t);
                    } catch (InterruptedException e) {
                    }
                }

                System.out.println("Finish " + Thread.currentThread().getName());
            }
        };
    }

}
