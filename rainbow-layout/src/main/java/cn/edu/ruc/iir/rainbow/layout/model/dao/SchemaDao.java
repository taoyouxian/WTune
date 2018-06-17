package cn.edu.ruc.iir.rainbow.layout.model.dao;

import cn.edu.ruc.iir.rainbow.common.DBUtil;
import cn.edu.ruc.iir.rainbow.common.LogFactory;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Schema;
import org.apache.commons.logging.Log;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SchemaDao implements Dao<Schema>
{
    public SchemaDao() {}

    private static final DBUtil db = DBUtil.Instance();
    private static final Log log = LogFactory.Instance().getLog();

    @Override
    public Schema getById(int id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT DB_NAME, DB_DESC FROM DBS WHERE DB_ID=" + id);
            if (rs.next())
            {
                Schema schema = new Schema();
                schema.setId(id);
                schema.setName(rs.getString("DB_NAME"));
                schema.setDesc(rs.getString("DB_DESC"));
                return schema;
            }

        } catch (SQLException e)
        {
            log.error("getById in SchemaDao", e);
        }

        return null;
    }

    public Schema getByName(String name)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT DB_ID, DB_DESC FROM DBS WHERE DB_NAME='" + name + "'");
            if (rs.next())
            {
                Schema schema = new Schema();
                schema.setId(rs.getInt("DB_ID"));
                schema.setName(name);
                schema.setDesc(rs.getString("DB_DESC"));
                return schema;
            }

        } catch (SQLException e)
        {
            log.error("getByName in SchemaDao", e);
        }

        return null;
    }
}
