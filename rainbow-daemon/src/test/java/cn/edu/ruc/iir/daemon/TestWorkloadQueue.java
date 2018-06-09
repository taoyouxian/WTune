package cn.edu.ruc.iir.daemon;

import cn.edu.ruc.iir.rainbow.daemon.workload.WorkloadQueue;
import org.junit.Test;

import java.util.HashSet;

public class TestWorkloadQueue
{
    class Consumer implements Runnable
    {
        private WorkloadQueue buffer;

        public Consumer (WorkloadQueue buffer)
        {
            this.buffer = buffer;
        }

        @Override
        public void run()
        {
            while (true)
            {
                try
                {
                    this.buffer.pop();
                    Thread.sleep(100);
                    System.out.println("consume a workload");

                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    class Producer implements Runnable
    {
        private WorkloadQueue buffer;

        public Producer (WorkloadQueue buffer)
        {
            this.buffer = buffer;
        }

        @Override
        public void run()
        {
            while (true)
            {
                try
                {
                    this.buffer.push(new HashSet<>());
                    this.buffer.push(new HashSet<>());
                    System.out.println("produce 2 workloads");
                    Thread.sleep(1000);
                } catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void test () throws InterruptedException
    {
        WorkloadQueue buffer = new WorkloadQueue();

        Consumer consumer1 = new Consumer(buffer);
        Consumer consumer2 = new Consumer(buffer);
        Producer producer = new Producer(buffer);

        Thread thread1 = new Thread(consumer1);
        Thread thread2 = new Thread(consumer2);
        Thread thread3 = new Thread(producer);

        //System.out.println(11);
        thread1.start();
        thread2.start();
        //Thread.sleep(1000);
        thread3.start();
        thread1.join();
        thread2.join();
        thread3.join();
    }
}
