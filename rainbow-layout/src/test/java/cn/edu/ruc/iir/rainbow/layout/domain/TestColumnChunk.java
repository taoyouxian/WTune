package cn.edu.ruc.iir.rainbow.layout.domain;

import cn.edu.ruc.iir.rainbow.layout.domian.ColumnChunk;
import org.junit.Test;

public class TestColumnChunk
{
    @Test
    public void test ()
    {
        ColumnChunk columnChunk = new ColumnChunk(1,2,3,"c1","int",123);
        System.out.println(columnChunk.getColumnId());
        System.out.println(columnChunk.getRowGroupId());
        System.out.println(columnChunk.getId());
        System.out.println(columnChunk.hashCode());
        ColumnChunk columnChunk1 = columnChunk.clone();
        System.out.println(columnChunk1.getColumnId());
        System.out.println(columnChunk1.getRowGroupId());
        System.out.println(columnChunk1.getId());
        System.out.println(columnChunk1.hashCode());

        System.out.println(columnChunk.compareTo(columnChunk1));
        System.out.println(columnChunk.equals(columnChunk1));

        columnChunk1.setRowGroupId(3);
        System.out.println(columnChunk.compareTo(columnChunk1));
        System.out.println(columnChunk.equals(columnChunk1));
        System.out.println(columnChunk1.getRowGroupId());
        System.out.println(columnChunk1.getColumnId());
        System.out.println(columnChunk1.getId());
    }
}
