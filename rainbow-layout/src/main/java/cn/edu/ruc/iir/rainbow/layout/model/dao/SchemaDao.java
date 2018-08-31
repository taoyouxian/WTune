package cn.edu.ruc.iir.rainbow.layout.model.dao;

import cn.edu.ruc.iir.rainbow.common.DBUtil;
import cn.edu.ruc.iir.rainbow.common.LogFactory;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Schema;
import org.apache.commons.logging.Log;

import java.sql.*;

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

    public boolean insert (Schema schema)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO DBS(" +
                "`DB_NAME`," +
                "`DB_DESC`) VALUES (?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, schema.getName());
            pst.setString(2, schema.getDesc());
            return pst.execute();
        } catch (SQLException e)
        {
            log.error("insert in SchemaDao", e);
        }
        return false;
    }
}
