package cn.edu.ruc.iir.rainbow.daemon.layout;

import cn.edu.ruc.iir.rainbow.daemon.Server;

import java.util.concurrent.TimeUnit;

public class LayoutServer implements Server
{
    private boolean shutdown = true;
    private String tableName;

    public LayoutServer (String tableName)
    {
        this.tableName = tableName;
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
            System.out.println("layout server [" + this.tableName + "] is running...");
            try
            {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }
}
