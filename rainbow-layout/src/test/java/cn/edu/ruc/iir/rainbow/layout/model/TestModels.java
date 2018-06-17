package cn.edu.ruc.iir.rainbow.layout.model;

import cn.edu.ruc.iir.rainbow.layout.model.domain.Column;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Layout;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Schema;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Table;
import org.junit.Test;

import java.util.List;

public class TestModels
{
    @Test
    public void testSchema ()
    {
        SchemaModel schemaModel = new SchemaModel();
        Schema schema = schemaModel.getByName("pixels");
        System.out.println(schema.getId() + ", " + schema.getName() + ", " + schema.getDesc());
    }

    @Test
    public void testTable ()
    {
        TableModel tableModel = new TableModel();
        List<Table> tables = tableModel.getByName("test");
        for (Table table : tables)
        {
            System.out.println(table.getId() + ", " + table.getSchema().getName());
        }
    }

    @Test
    public void testLayout ()
    {
        String schemaName = "pixels";
        String tableName = "test30g_pixels";

        SchemaModel schemaModel = new SchemaModel();
        TableModel tableModel = new TableModel();
        ColumnModel columnModel = new ColumnModel();
        LayoutModel layoutModel = new LayoutModel();

        Schema schema = schemaModel.getByName(schemaName);
        Table table = tableModel.getByNameAndSchema(tableName, schema);
        columnModel.getByTable(table);
        layoutModel.getByTable(table);

        for (Column column : table.getColumns())
        {
            System.out.println(column.getName() + ", " + column.getType());
        }

        for (Layout layout : table.getLayouts())
        {
            System.out.println(layout.getOrderPath());
        }
    }
}
