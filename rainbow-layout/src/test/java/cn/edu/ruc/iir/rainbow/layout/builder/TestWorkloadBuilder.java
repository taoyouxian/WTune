package cn.edu.ruc.iir.rainbow.layout.builder;

import cn.edu.ruc.iir.rainbow.common.exception.ColumnNotFoundException;
import cn.edu.ruc.iir.rainbow.layout.TestScoa;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class TestWorkloadBuilder
{
    @Test
    public void test () throws IOException, ColumnNotFoundException
    {
        List<Column> initColumnOrder = ColumnOrderBuilder.build(new File(TestScoa.class.getResource("/schema.txt").getFile()));
        List<Query> workload = WorkloadBuilder.build(new File(TestScoa.class.getResource("/workload.txt").getFile()), initColumnOrder);
        WorkloadBuilder.saveAsWorkloadFile(new File("123.txt"), workload, initColumnOrder);
    }
}
