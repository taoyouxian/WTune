package cn.edu.ruc.iir.rainbow.daemon;

public class WorkloadServer implements Server
{
    @Override
    public boolean isRunning()
    {
        return Thread.currentThread().isAlive();
    }

    @Override
    public void shutdown()
    {

    }

    @Override
    public void run()
    {

    }
}
