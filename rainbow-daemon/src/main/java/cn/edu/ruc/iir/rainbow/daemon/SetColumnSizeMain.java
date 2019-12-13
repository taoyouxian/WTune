package cn.edu.ruc.iir.rainbow.daemon;

import cn.edu.ruc.iir.pixels.common.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Table;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.ColumnDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.SchemaDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.TableDao;
import cn.edu.ruc.iir.rainbow.common.exception.MetadataException;
import cn.edu.ruc.iir.rainbow.common.metadata.PixelsMetadataStat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created at: 18-12-1
 * Author: hank
 */
public class SetColumnSizeMain
{
    public static void main(String[] args) throws IOException, MetadataException
    {
        SchemaDao schemaModel = new SchemaDao();
        TableDao tableModel = new TableDao();
        ColumnDao columnModel = new ColumnDao();
        Schema schema = schemaModel.getByName("pixels");
        Table table = tableModel.getByNameAndSchema(args[0], schema);
        List<Column> columns = columnModel.getByTable(table);
        Map<String, Column> nameToColumnMap = new HashMap<>();

        for (Column column : columns)
        {
            nameToColumnMap.put(column.getName(), column);
        }

        System.out.println(columns.size());

        PixelsMetadataStat stat = new PixelsMetadataStat(args[1], 9000, args[2]);
        double[] columnSizes = stat.getAvgColumnChunkSize();
        List<String> columnNames = stat.getFieldNames();
        for (int i = 0;i < columnNames.size(); ++i)
        {
            String name = columnNames.get(i);
            nameToColumnMap.get(name).setSize(columnSizes[i]);
        }

        for (Column column : columns)
        {
            columnModel.update(column);
        }
    }
}
