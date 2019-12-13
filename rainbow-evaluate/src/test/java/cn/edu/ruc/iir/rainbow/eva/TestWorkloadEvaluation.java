package cn.edu.ruc.iir.rainbow.eva;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;
import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.eva.invoker.InvokerWorkloadEva;
import org.junit.Test;

import java.util.Properties;

public class TestWorkloadEvaluation
{
    @Test
    public void test()
    {
        ConfigFactory.Instance().loadProperties("/home/hank/Desktop/rainbow/rainbow-evaluate/rainbow.properties");
        Properties params = new Properties();
        params.setProperty("method", "PRESTO");
        params.setProperty("format", "ORC");
        params.setProperty("table.dir", "/rainbow2/orc");
        params.setProperty("table.name", "orc");
        params.setProperty("workload.file", "/home/hank/Desktop/rainbow/rainbow-evaluate/workload.txt");
        params.setProperty("log.dir", "/home/hank/Desktop/rainbow/rainbow-evaluate/workload_eva/");
        params.setProperty("drop.cache", "false");

        Invoker invoker = new InvokerWorkloadEva();
        try
        {
            invoker.executeCommands(params);
        } catch (InvokerException e)
        {
            e.printStackTrace();
        }
    }
}
