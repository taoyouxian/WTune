package cn.edu.ruc.iir.rainbow.layout.domain;

import cn.edu.ruc.iir.rainbow.layout.domian.Columnlet;
import org.junit.Test;

public class TestColumnlet
{
    @Test
    public void test ()
    {
        Columnlet columnlet = new Columnlet(1,2,3,"c1","int",123);
        System.out.println(columnlet.getColumnId());
        System.out.println(columnlet.getRowGroupId());
        System.out.println(columnlet.getId());
        System.out.println(columnlet.hashCode());
        Columnlet columnlet1 = columnlet.clone();
        System.out.println(columnlet1.getColumnId());
        System.out.println(columnlet1.getRowGroupId());
        System.out.println(columnlet1.getId());
        System.out.println(columnlet1.hashCode());

        System.out.println(columnlet.compareTo(columnlet1));
        System.out.println(columnlet.equals(columnlet1));

        columnlet1.setRowGroupId(3);
        System.out.println(columnlet.compareTo(columnlet1));
        System.out.println(columnlet.equals(columnlet1));
        System.out.println(columnlet1.getRowGroupId());
        System.out.println(columnlet1.getColumnId());
        System.out.println(columnlet1.getId());
    }
}
