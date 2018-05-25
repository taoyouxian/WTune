package cn.edu.ruc.iir.rainbow.layout.model;

import cn.edu.ruc.iir.rainbow.common.DBUtil;
import cn.edu.ruc.iir.rainbow.common.LogFactory;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Layout;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Table;
import org.apache.commons.logging.Log;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class LayoutModel implements Model<Layout>
{
    public LayoutModel () {}

    private static final DBUtil db = DBUtil.Instance();
    private static final Log log = LogFactory.Instance().getLog();
    private static final TableModel tableModel = new TableModel();

    @Override
    public Layout getById(int id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM LAYOUTS WHERE LAYOUT_ID=" + id);
            if (rs.next())
            {
                Layout layout = new Layout();
                layout.setId(id);
                layout.setVersion(rs.getInt("LAYOUT_VERSION"));
                layout.setActive(rs.getShort("LAYOUT_ACTIVE") == 1);
                layout.setEnabled(rs.getShort("LAYOUT_ENABLED") == 1);
                layout.setEnabledAt(rs.getLong("LAYOUT_ENABLED_AT"));
                layout.setCreateAt(rs.getLong("LAYOUT_CREATE_AT"));
                layout.setInitOrder(rs.getString("LAYOUT_INIT_ORDER"));
                layout.setInitPath(rs.getString("LAYOUT_INIT_PATH"));
                layout.setCompact(rs.getString("LAYOUT_COMPACT"));
                layout.setCompactPath(rs.getString("LAYOUT_COMPACT_PATH"));
                layout.setSplit(rs.getString("LAYOUT_SPLIT"));
                layout.setTable(tableModel.getById(rs.getInt("TBLS_TBL_ID")));
                return layout;
            }
        } catch (SQLException e)
        {
            log.error("getById in LayoutModel", e);
        }

        return null;
    }

    public List<Layout> getByTable (Table table)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM LAYOUTS WHERE TBLS_TBL_ID=" + table.getId());
            List<Layout> layouts = new ArrayList<>();
            while (rs.next())
            {
                Layout layout = new Layout();
                layout.setId(rs.getInt("LAYOUT_ID"));
                layout.setVersion(rs.getInt("LAYOUT_VERSION"));
                layout.setActive(rs.getShort("LAYOUT_ACTIVE") == 1);
                layout.setEnabled(rs.getShort("LAYOUT_ENABLED") == 1);
                layout.setEnabledAt(rs.getLong("LAYOUT_ENABLED_AT"));
                layout.setCreateAt(rs.getLong("LAYOUT_CREATE_AT"));
                layout.setInitOrder(rs.getString("LAYOUT_INIT_ORDER"));
                layout.setInitPath(rs.getString("LAYOUT_INIT_PATH"));
                layout.setCompact(rs.getString("LAYOUT_COMPACT"));
                layout.setCompactPath(rs.getString("LAYOUT_COMPACT_PATH"));
                layout.setSplit(rs.getString("LAYOUT_SPLIT"));
                layout.setTable(table);
                table.addLayout(layout);
                layouts.add(layout);
            }
            return layouts;
        } catch (SQLException e)
        {
            log.error("getById in LayoutModel", e);
        }

        return null;
    }

    public boolean save (Layout layout)
    {
        if (exists(layout))
        {
            return update(layout);
        }
        else
        {
            return insert(layout);
        }
    }

    public boolean exists (Layout layout)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT 1 FROM LAYOUTS WHERE LAYOUT_ID=" + layout.getId());
            if (rs.next())
            {
                return true;
            }
        } catch (SQLException e)
        {
            log.error("exists in LayoutModel", e);
        }

        return false;
    }

    private boolean insert (Layout layout)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO LAYOUTS(" +
                "`LAYOUT_VERSION`," +
                "`LAYOUT_CREATE_AT`," +
                "`LAYOUT_ACTIVE`," +
                "`LAYOUT_ENABLED`," +
                "`LAYOUT_ENABLED_AT`," +
                "`LAYOUT_INIT_ORDER`," +
                "`LAYOUT_INIT_PATH`," +
                "`LAYOUT_COMPACT`," +
                "`LAYOUT_COMPACT_PATH`," +
                "`LAYOUT_SPLIT`," +
                "`TBLS_TBL_ID`) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setInt(1, layout.getVersion());
            pst.setLong(2, layout.getCreateAt());
            pst.setShort(3, (short) (layout.isActive() ? 1 : 0));
            pst.setShort(4, (short) (layout.isEnabled() ? 1 : 0));
            pst.setLong(5, layout.getEnabledAt());
            pst.setString(6, layout.getInitOrder());
            pst.setString(7, layout.getInitPath());
            pst.setString(8, layout.getCompact());
            pst.setString(9, layout.getCompactPath());
            pst.setString(10, layout.getSplit());
            pst.setInt(11, layout.getTable().getId());
            return pst.execute();
        } catch (SQLException e)
        {
            log.error("insert in LayoutModel", e);
        }
        return false;
    }

    private boolean update (Layout layout)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE LAYOUTS\n" +
                "SET\n" +
                "`LAYOUT_VERSION` = ?," +
                "`LAYOUT_CREATE_AT` = ?," +
                "`LAYOUT_ACTIVE` = ?," +
                "`LAYOUT_ENABLED` = ?," +
                "`LAYOUT_ENABLED_AT` = ?," +
                "`LAYOUT_INIT_ORDER` = ?," +
                "`LAYOUT_INIT_PATH` = ?," +
                "`LAYOUT_COMPACT` = ?," +
                "`LAYOUT_COMPACT_PATH` = ?," +
                "`LAYOUT_SPLIT` = ?," +
                "`TBLS_TBL_ID` = ?\n" +
                "WHERE `LAYOUT_ID` = ?;";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setInt(1, layout.getVersion());
            pst.setLong(2, layout.getCreateAt());
            pst.setShort(3, (short) (layout.isActive() ? 1 : 0));
            pst.setShort(4, (short) (layout.isEnabled() ? 1 : 0));
            pst.setLong(5, layout.getEnabledAt());
            pst.setString(6, layout.getInitOrder());
            pst.setString(7, layout.getInitPath());
            pst.setString(8, layout.getCompact());
            pst.setString(9, layout.getCompactPath());
            pst.setString(10, layout.getSplit());
            pst.setInt(11, layout.getTable().getId());
            pst.setInt(12, layout.getId());
            return pst.execute();
        } catch (SQLException e)
        {
            log.error("insert in LayoutModel", e);
        }
        return false;
    }
}
