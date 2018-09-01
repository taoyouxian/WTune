package cn.edu.ruc.iir.rainbow.layout.model.dao;


import cn.edu.ruc.iir.rainbow.common.DBUtil;
import cn.edu.ruc.iir.rainbow.common.LogFactory;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Column;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Table;
import org.apache.commons.logging.Log;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ColumnDao implements Dao<Column>
{
    public ColumnDao() {}

    private static final DBUtil db = DBUtil.Instance();
    private static final Log log = LogFactory.Instance().getLog();
    private static final TableDao tableModel = new TableDao();

    @Override
    public Column getById(int id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT COL_NAME, COL_TYPE, COL_SIZE, TBLS_TBL_ID FROM COLS WHERE COL_ID=" + id);
            if (rs.next())
            {
                Column column = new Column();
                column.setId(id);
                column.setName(rs.getString("COL_NAME"));
                column.setType(rs.getString("COL_TYPE"));
                column.setSize(rs.getDouble("COL_SIZE"));
                column.setTable(tableModel.getById(rs.getInt("TBLS_TBL_ID")));
                return column;
            }

        } catch (SQLException e)
        {
            log.error("getById in ColumnDao", e);
        }

        return null;
    }

    @Override
    public List<Column> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    public List<Column> getByTable(Table table)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT COL_ID, COL_NAME, COL_TYPE, COL_SIZE FROM COLS WHERE TBLS_TBL_ID=" + table.getId() +
            " ORDER BY COL_ID");
            List<Column> columns = new ArrayList<>();
            while (rs.next())
            {
                Column column = new Column();
                column.setId(rs.getInt("COL_ID"));
                column.setName(rs.getString("COL_NAME"));
                column.setType(rs.getString("COL_TYPE"));
                column.setSize(rs.getDouble("COL_SIZE"));
                column.setTable(table);
                table.addColumn(column);
                columns.add(column);
            }
            return columns;

        } catch (SQLException e)
        {
            log.error("getByTable in ColumnDao", e);
        }

        return null;
    }

    public boolean update(Column column)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE COLS\n" +
                "SET\n" +
                "`COL_NAME` = ?," +
                "`COL_TYPE` = ?," +
                "`COL_SIZE` = ?\n" +
                "WHERE `COL_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, column.getName());
            pst.setString(2, column.getType());
            pst.setDouble(3, column.getSize());
            pst.setInt(4, column.getId());

            return pst.execute();
        } catch (SQLException e)
        {
            log.error("getByTable in ColumnDao", e);
        }

        return false;
    }

    public int insertBatch (Table table, List<Column> columns)
    {
        StringBuilder sql = new StringBuilder("INSERT INTO COLS (COL_NAME,COL_TYPE,COL_SIZE,TBLS_TBL_ID)" +
                "VALUES ");
        for (Column column : columns)
        {
            sql.append("('").append(column.getName()).append("','").append(column.getType())
                    .append("',").append(column.getSize()).append(",").append(table.getId()).append("),");
        }
        sql.deleteCharAt(sql.length()-1);
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            return st.executeUpdate(sql.toString());
        } catch (SQLException e)
        {
            log.error("insertBatch in ColumnDao", e);
        }
        return 0;
    }

    public boolean deleteByTable (Table table)
    {
        Connection conn = db.getConnection();
        String sql = "DELETE FROM COLS WHERE TBLS_TBL_ID=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setInt(1, table.getId());
            return pst.execute();
        } catch (SQLException e)
        {
            log.error("delete in ColumnDao", e);
        }
        return false;
    }
}
