package cn.edu.ruc.iir.rainbow.seek;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;
import cn.edu.ruc.iir.rainbow.layout.invoker.InvokerPERFESTIMATION;
import org.junit.Test;

import java.util.Properties;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.seek
 * @ClassName: CostAnalyzer
 * @Description:
 * @author: tao
 * @date: Create in 2019-12-13 19:39
 **/
public class CostAnalyzer {

    @Test
    public void generateEstimation() {
        String path = ConfigFactory.Instance().getProperty("tune.path");
        String estimatedDuration = path + "rl/estimate_duration.csv";
        String estimatedDuration_Ordered = path + "rl/estimate_duration_ordered.csv";
        String joinEstimatedDuration = path + "rl/estimate_duration_joined.csv";

        Invoker invoker = new InvokerPERFESTIMATION();
        Properties params = new Properties();
        params.setProperty("workload.file", path + "rl/workload.txt");
        params.setProperty("schema.file", path + "rl/schema.txt");
        params.setProperty("log.file", estimatedDuration);
        params.setProperty("num.row.group", "128");
        params.setProperty("seek.cost.function", "power");
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }

        // ordered
        params.setProperty("schema.file", path + "rl/schema_ordered.txt");
        params.setProperty("log.file", estimatedDuration_Ordered);
        params.setProperty("num.row.group", "128");
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }

        // join two estimated duration
        AnalyzerUtil.joinEstimatedDuration(estimatedDuration, estimatedDuration_Ordered, joinEstimatedDuration, false);
    }

    @Test
    public void getEstimationTotal() {
        String path = ConfigFactory.Instance().getProperty("tune.path");
        String estimatedDuration = path + "rl/estimate_duration.csv";

        Invoker invoker = new InvokerPERFESTIMATION();
        Properties params = new Properties();
        params.setProperty("workload.file", path + "rl/workload.txt");
        params.setProperty("schema.file", path + "rl/schema.txt");
        params.setProperty("log.file", estimatedDuration);
        params.setProperty("num.row.group", "128");
        params.setProperty("seek.cost.function", "power");
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }
        double totalCost = AnalyzerUtil.getTotalEstimatedDuration(estimatedDuration, 1000);
        System.out.println("total cost:" + totalCost);
        String estimatedDuration_Ordered = path + "rl/estimate_duration_ordered.csv";
        double totalCost_Ordered = AnalyzerUtil.getTotalEstimatedDuration(estimatedDuration_Ordered, 1000);
        System.out.println("total cost:" + totalCost_Ordered);
    }

}
