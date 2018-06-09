package cn.edu.ruc.iir.rainbow.layout.builder;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.layout.builder.domain.CompactLayoutObj;
import cn.edu.ruc.iir.rainbow.layout.builder.domain.InitOrderObj;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Columnlet;
import com.alibaba.fastjson.JSON;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hank on 2015/4/28.
 */
public class ColumnOrderBuilder
{
    private ColumnOrderBuilder () {}

    public static List<Column> build (File columnOrderFile) throws IOException
    {
        BufferedReader reader = new BufferedReader(new FileReader(columnOrderFile));

        List<Column> columnOrder = new ArrayList<Column>();
        String line;
        int cid = 0;
        while ((line = reader.readLine()) != null)
        {
            String[] tokens = line.split("\t");
            Column column = new Column(cid, tokens[0], tokens[1], Double.parseDouble(tokens[2]));
            columnOrder.add(column);
            ++cid;
        }

        reader.close();

        return columnOrder;
    }

    public static List<Column> wrappedColumns (List<cn.edu.ruc.iir.rainbow.layout.model.domain.Column> columns)
    {
        List<Column> columnOrder = new ArrayList<>();
        int id = 0;
        for (cn.edu.ruc.iir.rainbow.layout.model.domain.Column column : columns)
        {
            Column column1 = new Column(id++, column.getName(), column.getType(), column.getSize());
            columnOrder.add(column1);
        }
        return columnOrder;
    }

    public static void saveAsSchemaFile (File columnOrderFile, List<Column> columnOrder) throws IOException
    {
        BufferedWriter writer = new BufferedWriter(new FileWriter(columnOrderFile));

        final String DUP_MARK = ConfigFactory.Instance().getProperty("dup.mark");

        for (Column column : columnOrder)
        {
            String columnName = column.getName();
            if (column.isDuplicated())
            {
                columnName += DUP_MARK + column.getDupId();
            }
            writer.write(columnName + "\t" + column.getType() + "\t" + column.getSize() + "\n");
        }

        writer.close();
    }

    public static String initOrderToJsonString (List<Column> columnOrder)
    {
        InitOrderObj initOrder = new InitOrderObj();

        for (Column column : columnOrder)
        {
            initOrder.addColumnOrder(column.getName());
        }

        return JSON.toJSONString(initOrder);
    }

    public static String compactLayoutToJsonString (int rowGroupNumber, int columnNumner, List<Column> columnletOrder)
    {
        CompactLayoutObj compactLayout = new CompactLayoutObj();
        compactLayout.setRowGroupNumber(rowGroupNumber);
        compactLayout.setColumnNumber(columnNumner);
        for (Column column : columnletOrder)
        {
            Columnlet columnlet = (Columnlet) column;
            String columnletStr = columnlet.getRowGroupId() + ":" + columnlet.getColumnId();
            compactLayout.addColumnletOrder(columnletStr);
        }

        return JSON.toJSONString(compactLayout);
    }
}
