package cn.edu.ruc.iir.rainbow.seek;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.layout.cmd.CmdOrdering;
import org.junit.Test;

import java.util.Properties;

import static cn.edu.ruc.iir.rainbow.seek.Constants.*;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.seek
 * @ClassName: LayoutAnalyzer
 * @Description:
 * @author: tao
 * @date: Create in 2019-12-14 14:36
 **/
public class LayoutAnalyzer {

    @Test
    public void ordering() {
        String path = ConfigFactory.Instance().getProperty("tune.path");
        Properties params = new Properties();
        params.setProperty("algorithm.name", "scoa.gs");
        params.setProperty("schema.file", default_SchemaPath);
        params.setProperty("workload.file", default_WorkloadPath);
        params.setProperty("ordered.schema.file", layout_Ordered);
        params.setProperty("seek.cost.function", "power");
        params.setProperty("computation.budget", "200");
        params.setProperty("num.row.group", "100");
        params.setProperty("row.group.size", "134217728");

        Command command = new CmdOrdering();
        command.setReceiver(new Receiver() {
            @Override
            public void progress(double percentage) {
                System.out.println("Ordering: " + ((int) (percentage * 10000) / 100.0) + "%");
            }

            @Override
            public void action(Properties results) {
                System.out.println("cmd:" + results.getProperty("success"));
                System.out.println("init.cost:" + results.getProperty("init.cost"));
                System.out.println("ordered.schema.file:" + results.getProperty("ordered.schema.file"));
                System.out.println("final.cost:" + results.getProperty("final.cost"));
                System.out.println("row.group.size:" + Double.valueOf(results.getProperty("row.group.size")) / 1024 / 1024);
                System.out.println("num.row.group:" + results.getProperty("num.row.group"));
                System.out.println("\nOrdering OK.");
            }
        });
        command.execute(params);
    }
}
