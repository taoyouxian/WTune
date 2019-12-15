package cn.edu.ruc.iir.rainbow.seek;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;
import cn.edu.ruc.iir.rainbow.layout.invoker.InvokerPERFESTIMATION;
import org.junit.Test;

import java.util.Properties;

import static cn.edu.ruc.iir.rainbow.seek.Constants.*;

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
        Invoker invoker = new InvokerPERFESTIMATION();
        Properties params = new Properties();
        params.setProperty("workload.file", default_WorkloadPath);
        params.setProperty("schema.file", default_SchemaPath);
        params.setProperty("log.file", estimate_Duration_Path);
        params.setProperty("num.row.group", "128");
        params.setProperty("seek.cost.function", "power");
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }

        // ordered
        params.setProperty("schema.file", schemaPath_Ordered);
        params.setProperty("log.file", estimated_Duration_Path_Ordered);
        params.setProperty("num.row.group", "128");
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }

        // join two estimated duration
        AnalyzerUtil.joinEstimatedDuration(estimate_Duration_Path, estimated_Duration_Path_Ordered, joined_Estimated_Duration_Path, false);
    }

    @Test
    public void getEstimationTotal() {
        Invoker invoker = new InvokerPERFESTIMATION();
        Properties params = new Properties();
        params.setProperty("workload.file", default_WorkloadPath);
        params.setProperty("schema.file", default_SchemaPath);
        params.setProperty("log.file", estimate_Duration_Path);
        params.setProperty("num.row.group", "128");
        params.setProperty("seek.cost.function", "power");
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }
        double totalCost = AnalyzerUtil.getTotalEstimatedDuration(estimate_Duration_Path, 1000);
        System.out.println("total cost:" + totalCost);
        double totalCost_Ordered = AnalyzerUtil.getTotalEstimatedDuration(estimated_Duration_Path_Ordered, 1000);
        System.out.println("total cost:" + totalCost_Ordered);
    }

}
