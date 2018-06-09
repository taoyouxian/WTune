package cn.edu.ruc.iir.rainbow.daemon.workload;

import cn.edu.ruc.iir.rainbow.workload.cache.AccessPattern;

import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class WorkloadQueue
{
    private BlockingQueue<Set<AccessPattern>> queue = new ArrayBlockingQueue<>(MAX_BUFFER_SIZE, true);
    private static final int MAX_BUFFER_SIZE = 256;

    public void push (Set<AccessPattern> workload) throws InterruptedException
    {
        this.queue.put(workload);
    }

    public Set<AccessPattern> pop () throws InterruptedException
    {
            Set<AccessPattern> res = this.queue.take();
            return res;
    }
}
