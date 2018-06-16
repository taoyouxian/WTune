package cn.edu.ruc.iir.rainbow.layout.builder;

import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Columnlet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestColumnOrderBuilder
{
    @Test
    public void test ()
    {
        List<Column> initOrder = new ArrayList<>();
        initOrder.add(new Column(1, "Column_0", "int", 1));
        initOrder.add(new Column(1, "Column_1", "int", 1));
        initOrder.add(new Column(1, "Column_2", "int", 1));
        String json = ColumnOrderBuilder.orderToJsonString(initOrder);
        System.out.println(json);

        List<Column> compact = new ArrayList<>();
        compact.add(new Columnlet(0, 1, 3, "Column_1", "int", 1));
        compact.add(new Columnlet(1, 2, 3, "Column_2", "int", 1));
        compact.add(new Columnlet(0, 0, 3, "Column_0", "int", 1));
        compact.add(new Columnlet(1, 0, 3, "Column_0", "int", 1));
        compact.add(new Columnlet(1, 1, 3, "Column_1", "int", 1));
        compact.add(new Columnlet(0, 2, 3, "Column_2", "int", 1));
        json = ColumnOrderBuilder.compactLayoutToJsonString(2, 3, 1, compact);
        System.out.println(json);
    }
}
