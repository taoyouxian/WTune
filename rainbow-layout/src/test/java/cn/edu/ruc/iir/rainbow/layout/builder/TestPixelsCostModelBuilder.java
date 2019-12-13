package cn.edu.ruc.iir.rainbow.layout.builder;

import cn.edu.ruc.iir.rainbow.layout.builder.domain.BytesMsItem;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestPixelsCostModelBuilder
{
    @Test
    public void testPadding ()
    {
        List<BytesMsItem> bytesMsItems = new ArrayList<>();
        bytesMsItems.add(new BytesMsItem(0, 0));
        bytesMsItems.add(new BytesMsItem(100, 3));
        bytesMsItems.add(new BytesMsItem(200, 5));
        bytesMsItems.add(new BytesMsItem(300, 8));
        bytesMsItems.add(new BytesMsItem(400, 9));
        bytesMsItems.add(new BytesMsItem(500, 10));
        bytesMsItems.add(new BytesMsItem(600, 11));
        bytesMsItems.add(new BytesMsItem(700, 11.5));
        bytesMsItems.add(new BytesMsItem(800, 12));
        PixelsCostModelBuilder.padding(bytesMsItems, 100);

        System.out.println(bytesMsItems.size());
        for (BytesMsItem item : bytesMsItems)
        {
            System.out.println(item.getBytes() + ", " + item.getMs());
        }
    }
}
