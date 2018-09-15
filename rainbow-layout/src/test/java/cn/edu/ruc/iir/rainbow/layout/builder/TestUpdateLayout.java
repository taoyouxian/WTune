package cn.edu.ruc.iir.rainbow.layout.builder;

import cn.edu.ruc.iir.rainbow.layout.builder.domain.CompactLayoutObj;
import cn.edu.ruc.iir.rainbow.layout.model.dao.ColumnDao;
import cn.edu.ruc.iir.rainbow.layout.model.dao.LayoutDao;
import cn.edu.ruc.iir.rainbow.layout.model.dao.SchemaDao;
import cn.edu.ruc.iir.rainbow.layout.model.dao.TableDao;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Layout;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Schema;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Table;
import com.alibaba.fastjson.JSON;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestUpdateLayout
{
    @Test
    public void test ()
    {
        SchemaDao schemaModel = new SchemaDao();
        TableDao tableModel = new TableDao();
        LayoutDao layoutModel = new LayoutDao();
        ColumnDao columnModel = new ColumnDao();
        Schema schema = schemaModel.getByName("pixels");
        Table table = tableModel.getByNameAndSchema("test_105", schema);
        List<Layout> layouts = layoutModel.getByTable(table);

        for (Layout layout : layouts)
        {
            if (layout.getId()==1)
            {
                System.out.println(111);
                CompactLayoutObj compactObj = new CompactLayoutObj();
                compactObj.setNumColumn(105);
                compactObj.setNumRowGroupInBlock(16);
                compactObj.setCacheBorder(0);
                List<String> columnletOrder = new ArrayList<>();
                for (int i = 0; i < 105; ++i)
                {
                    for (int j = 0; j < 16; ++j)
                    {
                        String columnlet = i + ":" + j;
                        columnletOrder.add(columnlet);
                    }
                }
                compactObj.setColumnletOrder(columnletOrder);
                layout.setCompactPath("hdfs://dbiir01:9000/pixels/pixels/test_105/v_0_compact");
                layout.setCompact(JSON.toJSONString(compactObj));
                layoutModel.update(layout);
                break;
            }
        }
    }
}
