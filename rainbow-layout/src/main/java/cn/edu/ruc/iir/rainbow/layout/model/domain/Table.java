package cn.edu.ruc.iir.rainbow.layout.model.domain;

import java.util.List;

public class Table
{
    private int id;
    private String name;
    private String type;
    private Schema schema;
    private List<Column> columns;
    private List<Layout> layouts;

    public int getId()
    {
        return id;
    }

    public void setId(int id)
    {
        this.id = id;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getType()
    {
        return type;
    }

    public void setType(String type)
    {
        this.type = type;
    }

    public Schema getSchema()
    {
        return schema;
    }

    public void setSchema(Schema schema)
    {
        this.schema = schema;
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public void setColumns(List<Column> columns)
    {
        this.columns = columns;
    }

    public List<Layout> getLayouts()
    {
        return layouts;
    }

    public void setLayouts(List<Layout> layouts)
    {
        this.layouts = layouts;
    }
}
