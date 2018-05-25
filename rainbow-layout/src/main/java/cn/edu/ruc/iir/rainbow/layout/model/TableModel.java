package cn.edu.ruc.iir.rainbow.layout.model;

import cn.edu.ruc.iir.rainbow.common.DBUtil;
import cn.edu.ruc.iir.rainbow.common.LogFactory;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Schema;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Table;
import org.apache.commons.logging.Log;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TableModel implements Model<Table>
{
    public TableModel () {}

    private static final DBUtil db = DBUtil.Instance();
    private static final Log log = LogFactory.Instance().getLog();
    private static final SchemaModel schemaModel = new SchemaModel();

    @Override
    public Table getById(int id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT TBL_NAME, TBL_TYPE, DBS_DB_ID FROM TBLS WHERE TBL_ID=" + id);
            if (rs.next())
            {
                Table table = new Table();
                table.setId(id);
                table.setName(rs.getString("TBL_NAME"));
                table.setType(rs.getString("TBL_TYPE"));
                table.setSchema(schemaModel.getById(rs.getInt("DBS_DB_ID")));
                return table;
            }

        } catch (SQLException e)
        {
            log.error("getById in TableModel", e);
        }

        return null;
    }

    public Table getByNameAndSchema (String name, Schema schema)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT TBL_ID, TBL_TYPE FROM TBLS WHERE TBL_NAME='" + name +
                    "' AND DBS_DB_ID=" + schema.getId());
            if (rs.next())
            {
                Table table = new Table();
                table.setId(rs.getInt("TBL_ID"));
                table.setName(name);
                table.setType(rs.getString("TBL_TYPE"));
                table.setSchema(schema);
                schema.addTable(table);
                return table;
            }

        } catch (SQLException e)
        {
            log.error("getByNameAndDB in TableModel", e);
        }

        return null;
    }

    public List<Table> getByName(String name)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT TBL_ID, TBL_TYPE, DBS_DB_ID FROM TBLS WHERE TBL_NAME='" + name + "'");
            List<Table> tables = new ArrayList<>();
            while (rs.next())
            {
                Table table = new Table();
                table.setId(rs.getInt("TBL_ID"));
                table.setName(name);
                table.setType(rs.getString("TBL_TYPE"));
                table.setSchema(schemaModel.getById(rs.getInt("DBS_DB_ID")));
                tables.add(table);
            }
            return tables;

        } catch (SQLException e)
        {
            log.error("getByName in TableModel", e);
        }

        return null;
    }
}
