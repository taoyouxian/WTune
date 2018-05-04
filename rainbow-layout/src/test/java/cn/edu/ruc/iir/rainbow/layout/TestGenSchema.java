package cn.edu.ruc.iir.rainbow.layout;

import org.junit.Test;

import java.io.*;

public class TestGenSchema
{
    @Test
    public void test () throws IOException
    {
        BufferedReader readerSchema = new BufferedReader(new FileReader(TestGenSchema.class.getResource("/105_schema_origin.txt").getFile()));
        BufferedReader readerSize = new BufferedReader(new FileReader(TestGenSchema.class.getResource("/105_column_size.txt").getFile()));
        BufferedWriter writer = new BufferedWriter(new FileWriter("/home/hank/dev/idea-projects/rainbow/rainbow-layout/src/test/resources/105_schema.txt"));
        String sizeLine = null;
        String schemaLine = null;
        while ((sizeLine = readerSize.readLine()) != null &&
                (schemaLine = readerSchema.readLine()) != null)
        {
            String[] sizeSplit = sizeLine.split("\t");
            String[] schemaSplit = schemaLine.split(":");
            if (schemaSplit[0].equals(sizeSplit[0]))
            {
                writer.write(schemaSplit[0] + '\t' + schemaSplit[1].replace('\"', ' ').trim() + '\t' + sizeSplit[1]);
                writer.newLine();
            }
        }
        readerSchema.close();
        readerSize.close();
        writer.close();
    }
}
