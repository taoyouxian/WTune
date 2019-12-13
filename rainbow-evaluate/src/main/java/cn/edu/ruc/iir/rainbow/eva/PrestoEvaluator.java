package cn.edu.ruc.iir.rainbow.eva;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.eva.metrics.StageMetrics;

import java.sql.*;
import java.util.Properties;

public class PrestoEvaluator
{
    private static Connection connection = null;

    static
    {
        Properties properties = new Properties();
        String user = ConfigFactory.Instance().getProperty("presto.user");
        String password = ConfigFactory.Instance().getProperty("presto.password");
        String ssl = ConfigFactory.Instance().getProperty("presto.ssl");
        properties.setProperty("user", user);
        if (!password.equalsIgnoreCase("null"))
        {
            properties.setProperty("password", password);
        }
        properties.setProperty("SSL", ssl);
        String jdbcUrl = ConfigFactory.Instance().getProperty("presto.jdbc.url");
        try
        {
            Class.forName("com.facebook.presto.jdbc.PrestoDriver");
            connection = DriverManager.getConnection(jdbcUrl, properties);
        } catch (SQLException e)
        {
            e.printStackTrace();
        } catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }
    }

    public static StageMetrics execute (String tableName, String columns, String orderByColumn)
    {
        StageMetrics stageMetrics = new StageMetrics();

        try
        {
            Thread.sleep(1000);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        String sql = "";
        try (Statement statement = connection.createStatement())
        {
            sql = "select " + columns + " from " + tableName + " order by " + orderByColumn + " limit 10";
            long start = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            resultSet.next();
            stageMetrics.setDuration(System.currentTimeMillis() - start);
            stageMetrics.setId(0);
            statement.close();
        } catch (SQLException e)
        {
            e.printStackTrace();
            System.out.println("SQL: " + sql);
        }

        return stageMetrics;
    }
}
