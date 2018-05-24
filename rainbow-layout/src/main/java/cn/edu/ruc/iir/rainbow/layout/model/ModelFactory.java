package cn.edu.ruc.iir.rainbow.layout.model;

import java.util.HashMap;
import java.util.Map;

public class ModelFactory
{
    private static ModelFactory instance = new ModelFactory();

    public static ModelFactory Instance ()
    {
        return instance;
    }

    private Map<String, Model> modelMap= null;

    private ModelFactory ()
    {
        this.modelMap = new HashMap<>();
        this.modelMap.put("schema", new SchemaModel());
        this.modelMap.put("table", new TableModel());
        this.modelMap.put("column", new ColumnModel());
        this.modelMap.put("layout", new LayoutModel());
    }

    public Model getModel (String name)
    {
        return this.modelMap.get(name);
    }
}
