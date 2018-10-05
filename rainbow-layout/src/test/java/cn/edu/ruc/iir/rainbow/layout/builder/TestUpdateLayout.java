package cn.edu.ruc.iir.rainbow.layout.builder;

import cn.edu.ruc.iir.pixels.common.metadata.domain.Compact;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Order;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Table;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.ColumnDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.LayoutDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.SchemaDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.TableDao;
import com.alibaba.fastjson.JSON;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestUpdateLayout
{
    @Test
    public void testSetBasicCompactLayout ()
    {
        SchemaDao schemaDao = new SchemaDao();
        TableDao tableDao = new TableDao();
        LayoutDao layoutDao = new LayoutDao();
        ColumnDao columnDao = new ColumnDao();
        Schema schema = schemaDao.getByName("pixels");
        Table table = tableDao.getByNameAndSchema("test_105", schema);
        List<Layout> layouts = layoutDao.getByTable(table);

        for (Layout layout : layouts)
        {
            if (layout.getId()==1)
            {
                System.out.println("start...");
                Compact compactObj = new Compact();
                compactObj.setNumColumn(105);
                compactObj.setNumRowGroupInBlock(16);
                compactObj.setCacheBorder(0);
                List<String> columnletOrder = new ArrayList<>();
                for (int i = 0; i < 105; ++i)
                {
                    for (int j = 0; j < 16; ++j)
                    {
                        String columnlet = j + ":" + i;
                        columnletOrder.add(columnlet);
                    }
                }
                compactObj.setColumnletOrder(columnletOrder);
                layout.setCompactPath("hdfs://dbiir01:9000/pixels/pixels/test_105/v_0_compact");
                layout.setCompact(JSON.toJSONString(compactObj));
                layoutDao.update(layout);
                break;
            }
        }
    }

    @Test
    public void testCorrectCompactLayout()
    {
        SchemaDao schemaDao = new SchemaDao();
        TableDao tableDao = new TableDao();
        LayoutDao layoutDao = new LayoutDao();
        ColumnDao columnDao = new ColumnDao();
        Schema schema = schemaDao.getByName("pixels");
        Table table = tableDao.getByNameAndSchema("test_105", schema);
        List<Layout> layouts = layoutDao.getByTable(table);
        for (int i = 1; i < 2; ++i)
        {
            Layout currLayout = null;
            Layout nextLayout = null;
            for (Layout layout : layouts)
            {
                if (layout.getId() == i)
                {
                    currLayout = layout;
                }
                if (layout.getId() == i + 1)
                {
                    nextLayout = layout;
                }
            }

            Order currOrder = JSON.parseObject(currLayout.getOrder(), Order.class);
            Order nextOrder = JSON.parseObject(nextLayout.getOrder(), Order.class);
            Compact nextCompact = JSON.parseObject(nextLayout.getCompact(), Compact.class);

            int[] index = new int[currOrder.getColumnOrder().size()];

            for (int j = 0; j < currOrder.getColumnOrder().size(); ++j)
            {
                String currColumn = currOrder.getColumnOrder().get(j);
                for (int k = 0; k < nextOrder.getColumnOrder().size(); ++k)
                {
                    if (nextOrder.getColumnOrder().get(k).equals(currColumn))
                    {
                        index[j] = k;
                        break;
                    }
                }
            }

            List<String> correctColumnletOrder = new ArrayList<>();
            for (String wrongColumnlet : nextCompact.getColumnletOrder())
            {
                String[] split = wrongColumnlet.split(":");
                int wrongColumnId = Integer.parseInt(split[1]);
                String correctColumnlet = split[0] + ":" + index[wrongColumnId];
                correctColumnletOrder.add(correctColumnlet);
            }

            nextCompact.setColumnletOrder(correctColumnletOrder);
            nextLayout.setCompact(JSON.toJSONString(nextCompact));
            System.out.println(nextLayout.getCompact());
            //layoutDao.update(nextLayout);
        }
    }
}
