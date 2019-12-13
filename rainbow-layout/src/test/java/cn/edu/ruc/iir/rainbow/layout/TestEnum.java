package cn.edu.ruc.iir.rainbow.layout;

import cn.edu.ruc.iir.rainbow.layout.cost.SeekCost;
import org.junit.Test;

public class TestEnum
{
    @Test
    public void testEnumName ()
    {
        System.out.println(SeekCost.Type.POWER.toString());
        System.out.println(SeekCost.Type.POWER.name());
    }
}
