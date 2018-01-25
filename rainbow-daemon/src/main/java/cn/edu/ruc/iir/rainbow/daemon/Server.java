package cn.edu.ruc.iir.rainbow.daemon;

public interface Server extends Runnable
{
    public boolean isRunning();

    public void shutdown();
}
