package cn.edu.ruc.iir.rainbow.daemon.etl;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.metadata.MetadataService;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Order;
import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.daemon.Server;
import com.alibaba.fastjson.JSON;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ETLServer implements Server
{
    private boolean shutdown = true;
    private String schemaName;
    private String tableName;
    private List<Column> columns = null;
    private Layout writableLayout = null;
    private Order columnOrder = null;

    public ETLServer(String schemaName, String tableName) throws MetadataException
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        String host = ConfigFactory.Instance().getProperty("metadata.server.host");
        int port = Integer.parseInt(ConfigFactory.Instance().getProperty("metadata.server.port"));
        MetadataService metadataService = new MetadataService(host, port);
        // columns returned by metadataService is ordered by column ids.
        this.columns = metadataService.getColumns(this.schemaName, this.tableName);
        List<Layout> layouts = metadataService.getLayouts(this.schemaName, this.tableName);
        int writableVersion = -1;
        for (Layout layout : layouts)
        {
            if (layout.getPermission() > 0 && layout.getVersion() > writableVersion)
            {
                this.writableLayout = layout;
            }
        }

        if (this.columns == null || this.columns.isEmpty())
        {
            throw new MetadataException("no column in table " +
                    this.schemaName + "." + this.tableName);
        }

        if (this.writableLayout == null)
        {
            throw new MetadataException("no writable metadata for table " +
                    this.schemaName + "." + this.tableName);
        }

        this.columnOrder = JSON.parseObject(writableLayout.getOrder(), Order.class);
    }

    @Override
    public boolean isRunning()
    {
        return Thread.currentThread().isAlive();
    }

    @Override
    public void shutdown()
    {
        this.shutdown = true;
    }

    @Override
    public void run()
    {
        this.shutdown = false;
        while (this.shutdown == false)
        {
            System.out.println("ETL server [" + this.tableName + "] is running...");
            try
            {
                // read data from source and write pixels to order path.
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }
}
