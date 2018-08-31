package cn.edu.ruc.iir.rainbow.layout.sql;

import org.junit.Test;

import java.io.IOException;

public class TestGenSQL
{
    @Test
    public void genDDL () throws IOException
    {
        GenerateDDL.GenCreateOrc("t105_orc_ordered",
                "/home/hank/dev/idea-projects/rainbow/rainbow-layout/target/test-classes/105_scoa_ordered_schema.txt",
                "/home/hank/dev/idea-projects/rainbow/rainbow-layout/target/test-classes/t105_orc.sql");
    }

    @Test
    public void genLoad () throws IOException
    {
        GenerateLoad.Gen(true, "t105_orc_ordered", "/home/hank/dev/idea-projects/rainbow/rainbow-layout/target/test-classes/105_scoa_ordered_schema.txt",
                "/home/hank/dev/idea-projects/rainbow/rainbow-layout/target/test-classes/t105_orc_load.sql");
    }
}
