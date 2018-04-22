package cn.edu.ruc.iir.rainbow.layout;

import cn.edu.ruc.iir.rainbow.common.exception.*;
import cn.edu.ruc.iir.rainbow.layout.algorithm.AlgorithmFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.ExecutorContainer;
import cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord.FastScoaPixels;
import cn.edu.ruc.iir.rainbow.layout.builder.ColumnOrderBuilder;
import cn.edu.ruc.iir.rainbow.layout.builder.WorkloadBuilder;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestFastScoaPixels
{
    @Test
    public void test () throws IOException, ColumnNotFoundException, AlgoException, ClassNotFoundException, InterruptedException
    {
        List<Column> initColumnOrder = ColumnOrderBuilder.build(new File(TestScoa.class.getResource("/schema.txt").getFile()));
        List<Query> workload = WorkloadBuilder.build(new File(TestScoa.class.getResource("/workload.txt").getFile()), initColumnOrder);
        System.out.println(workload.size());
        //SeekCost seekCostFunction = new PowerSeekCost();
        //RealSeekCostBuilder.build(new File("layout/resources/seek_cost.txt"));

        FastScoaPixels scoaPixels = (FastScoaPixels) AlgorithmFactory.Instance().getAlgorithm("scoa.pixels", 10, new ArrayList<>(initColumnOrder), workload);
        System.out.println("Init cost: " + scoaPixels.getSchemaSeekCost());

        try
        {
            ExecutorContainer container = new ExecutorContainer(scoaPixels, 1);
            container.waitForCompletion(1, percentage -> {
                System.out.println(percentage);
            });
        } catch (NotMultiThreadedException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "thread number is " + 1, e);
        }

        System.out.println("Final cost: " + scoaPixels.getCurrentWorkloadSeekCost());
        ColumnOrderBuilder.saveAsSchemaFile(new File(TestScoa.class.getResource("/").getFile() + "scoa_pixels_ordered_schema1.txt"), scoaPixels.getColumnOrder());
        System.out.println("ordered schema file: " + TestScoa.class.getResource("/").getFile() + "scoa_pixels_ordered_schema1.txt");

    }
}
