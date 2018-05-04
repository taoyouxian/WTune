package cn.edu.ruc.iir.rainbow.layout;

import org.junit.Test;

import java.io.*;
import java.util.*;

public class TestGenWorkload
{
    @Test
    public void test () throws IOException
    {
        BufferedReader readerSchema = new BufferedReader(new FileReader(TestGenSchema.class.getResource("/105_schema.txt").getFile()));
        String line = null;
        Set<String> columns = new HashSet<>();
        while ((line = readerSchema.readLine()) != null)
        {
            columns.add(line.split("\t")[0]);
        }
        readerSchema.close();
        BufferedReader reader = new BufferedReader(new FileReader(TestGenSchema.class.getResource("/105_raw_accessed_column.txt").getFile()));
        BufferedWriter writer = new BufferedWriter(new FileWriter("/home/hank/dev/idea-projects/rainbow/rainbow-layout/src/test/resources/105_workload.txt"));
        line = reader.readLine();
        Map<String, List<String>> map = new TreeMap<>();
        while ((line = reader.readLine()) != null)
        {
            String[] split = line.split("\t");
            if (columns.contains(split[3]))
            {
                if (map.containsKey(split[0]))
                {
                    map.get(split[0]).add(split[3]);
                } else
                {
                    List<String> v = new ArrayList<>();
                    v.add(split[3]);
                    map.put(split[0], v);
                }
            }
        }
        reader.close();

        for (Map.Entry<String, List<String>> entry : map.entrySet())
        {
            writer.write(entry.getKey() + "\t1\t" + entry.getValue().get(0));

            for (int i = 1; i < entry.getValue().size(); ++i)
            {
                writer.write("," + entry.getValue().get(i));
            }
            writer.newLine();
        }

        writer.close();
    }
}
