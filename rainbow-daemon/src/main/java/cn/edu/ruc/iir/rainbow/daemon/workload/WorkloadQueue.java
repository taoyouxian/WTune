package cn.edu.ruc.iir.rainbow.daemon.workload;

import cn.edu.ruc.iir.rainbow.workload.cache.AccessPattern;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class WorkloadQueue
{
    private BlockingQueue<List<AccessPattern>> queue = new ArrayBlockingQueue<>(MAX_BUFFER_SIZE, true);
    private static final int MAX_BUFFER_SIZE = 256;

    public void push (List<AccessPattern> workload) throws InterruptedException
    {
        this.queue.put(workload);
    }

    public List<AccessPattern> pop () throws InterruptedException
    {
            List<AccessPattern> res = this.queue.take();
            return res;
    }
}
