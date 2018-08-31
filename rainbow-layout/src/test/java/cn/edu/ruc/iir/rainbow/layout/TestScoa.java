package cn.edu.ruc.iir.rainbow.layout;

import cn.edu.ruc.iir.rainbow.common.exception.*;
import cn.edu.ruc.iir.rainbow.layout.algorithm.Algorithm;
import cn.edu.ruc.iir.rainbow.layout.algorithm.AlgorithmFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.ExecutorContainer;
import cn.edu.ruc.iir.rainbow.layout.builder.ColumnOrderBuilder;
import cn.edu.ruc.iir.rainbow.layout.builder.WorkloadBuilder;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.rainbow.layout.cost.PowerSeekCost;
import cn.edu.ruc.iir.rainbow.layout.cost.SeekCost;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hank on 2015/4/28.
 */
public class TestScoa
{
    @Test
    public void testScoa() throws IOException, ColumnNotFoundException, AlgoException, ClassNotFoundException, InterruptedException
    {
        List<Column> initColumnOrder = ColumnOrderBuilder.build(new File(TestScoa.class.getResource("/105_schema.text").getFile()));
        List<Query> workload = WorkloadBuilder.build(new File(TestScoa.class.getResource("/105_workload.text").getFile()), initColumnOrder);
        System.out.println(workload.size());
        SeekCost seekCostFunction = new PowerSeekCost();
        //RealSeekCostBuilder.build(new File("layout/resources/seek_cost.txt"));

        Algorithm fastScoa = AlgorithmFactory.Instance().getAlgorithm("scoa", 200, new ArrayList<>(initColumnOrder), workload, seekCostFunction);
        System.out.println("Init cost: " + fastScoa.getSchemaSeekCost());
        try
        {
            ExecutorContainer container = new ExecutorContainer(fastScoa, 1);
            container.waitForCompletion(1, percentage -> {
                System.out.println(percentage);
            });
        } catch (NotMultiThreadedException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "thread number is " + 1, e);
        }

        System.out.println("Final cost: " + fastScoa.getCurrentWorkloadSeekCost());
        ColumnOrderBuilder.saveAsSchemaFile(new File(TestScoa.class.getResource("/").getFile() + "105_scoa_ordered_schema.txt"), fastScoa.getColumnOrder());
        System.out.println("ordered schema file: " + TestScoa.class.getResource("/").getFile() + "105_scoa_ordered_schema.txt");
    }
}
