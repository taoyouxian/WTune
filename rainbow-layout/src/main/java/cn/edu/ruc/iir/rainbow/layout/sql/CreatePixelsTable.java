package cn.edu.ruc.iir.rainbow.layout.sql;

import cn.edu.ruc.iir.rainbow.layout.model.dao.SchemaDao;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Schema;

public class CreatePixelsTable
{
    // insert metadata into pixels-metadata
    public static void createDB (String dbName)
    {
        Schema schema = new Schema();
        schema.setName(dbName);
        schema.setDesc("This schema is created by rainbow-layout.");
        SchemaDao schemaDao = new SchemaDao();
        schemaDao.insert(schema);
    }

    public static void main(String[] args)
    {
        createDB("pixels");
    }
}
