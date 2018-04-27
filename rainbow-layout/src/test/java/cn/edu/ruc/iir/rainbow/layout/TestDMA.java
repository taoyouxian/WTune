package cn.edu.ruc.iir.rainbow.layout;

import java.io.*;
import java.util.Random;

public class TestDMA
{
    private static void genFile () throws IOException
    {
        BufferedWriter writer = new BufferedWriter(new FileWriter("/home/hank/testfile"));
        Random random = new Random(System.nanoTime());
        for (int i = 0; i < 1024*1024*1024; i++)
        {
            writer.write((random.nextInt()%10000) + "\n");
        }
        writer.close();
    }

    private static String[] readBatch (BufferedReader reader, String[] a) throws IOException
    {
        for (int i = 0; i < 16; ++i)
        {
            a[i] = reader.readLine();
        }
        return a;
    }

    private static int max(String[] a)
    {
        int max = Integer.MIN_VALUE;
        for (int i = 0; i < a.length; ++i)
        {
            int ai = Integer.parseInt(a[i]);
            if (ai > max)
            {
                max = ai;
            }
        }
        return max;
    }

    private static int min(String[] a)
    {
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < a.length; ++i)
        {
            int ai = Integer.parseInt(a[i]);
            if (ai < min)
            {
                min = ai;
            }
        }
        return min;
    }

    public static void main(String[] args) throws IOException, InterruptedException
    {
        //genFile();

        /*
        long start = System.nanoTime();
        int[] a  = {4361, 860, 8334, 3310, 8337 -7929, 6575, -6825, 9373, -110,
                -2040, -198, -8604, 1007, -9685, -8613};

        BufferedReader reader = new BufferedReader(new FileReader("/home/hank/testfile"));


        for (long i = 0; i < 1024*1024*1024/16; i++)
        {
            readBatch(reader, a);
        }

        for (long i = 0; i < 1024*1024*1024/16; i++)
        {
            a = readBatch(reader, a);
            for (int j = 0; j < 16*100; ++j)
            {
                int min = min(a);
                int max = max(a);
            }
        }

        System.out.println((System.nanoTime()-start)/1000/1000);
        */

        long start = System.nanoTime();
        Thread thread0 = new Thread(new MyThread("/home/presto/testfile0"));
        Thread thread1 = new Thread(new MyThread("/home/presto/testfile1"));
        Thread thread2 = new Thread(new MyThread("/home/presto/testfile2"));
        Thread thread3 = new Thread(new MyThread("/home/presto/testfile3"));
        thread0.start();
        //thread1.start();
        //thread2.start();
        //thread3.start();
        thread0.join();
        //thread1.join();
        //thread2.join();
        //thread3.join();
        System.out.println((System.nanoTime() - start) / 1000 / 1000);

    }

    public static class MyThread implements Runnable
    {
        private String file = null;

        public MyThread (String file)
        {
            this.file = file;
        }

        @Override
        public void run()
        {
            try
            {
                long start = System.nanoTime();
                String[] a = new String[16];
                        //{4361, 860, 8334, 3310, 8337 - 7929, 6575, -6825, 9373, -110,
                        //-2040, -198, -8604, 1007, -9685, -8613};

                BufferedReader reader = new BufferedReader(new FileReader(this.file),1024*1024*10);


                for (long i = 0; i < 1024 * 1024 * 1024 / 16; i++)
                {
                    readBatch(reader, a);
                }


/*
                readBatch(reader, a);

                for (long i = 0; i < 1024 * 1024 * 1024 / 16; i++)
                {
                    //readBatch(reader, a);
                    for (int j = 0; j < 5; ++j)
                    {
                        int min = min(a);
                        int max = max(a);
                    }
                }
*/
                System.out.println(this.file + ": " + (System.nanoTime() - start) / 1000 / 1000);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }
}
