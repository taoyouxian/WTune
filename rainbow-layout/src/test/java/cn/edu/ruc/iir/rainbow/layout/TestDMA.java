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

    private static int[] readBatch (BufferedReader reader, int[] a) throws IOException
    {
        for (int i = 0; i < 16; ++i)
        {
            a[0] = Integer.parseInt(reader.readLine());
        }
        return a;
    }

    private static int max(int[] a)
    {
        int max = Integer.MIN_VALUE;
        for (int i = 0; i < a.length; ++i)
        {
            if (a[i] > max)
            {
                max = a[i];
            }
        }
        return max;
    }

    private static int min(int[] a)
    {
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < a.length; ++i)
        {
            if (a[i] < min)
            {
                min = a[i];
            }
        }
        return min;
    }

    public static void main(String[] args) throws IOException
    {
        genFile();

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

        System.out.println((System.nanoTime()-start)/1000/1000);*/
    }
}
