package cn.edu.ruc.iir.rainbow.layout.builder;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.HttpUtils;
import cn.edu.ruc.iir.rainbow.common.LogFactory;
import cn.edu.ruc.iir.rainbow.common.exception.CostFunctionException;
import cn.edu.ruc.iir.rainbow.layout.builder.domain.*;
import cn.edu.ruc.iir.rainbow.layout.cost.*;
import cn.edu.ruc.iir.rainbow.layout.cost.domain.Coordinate;
import com.alibaba.fastjson.JSON;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PixelsCostModelBuilder
{
    private PixelsCostModelBuilder() {}
    /**
     * some values in sortedItems could be missing, padding lost cost metrics into sortedItems.
     * sortedItems.size() >= 2
     * @param sortedItems costs to be padded
     */
    protected static void padding (List<BytesMsItem> sortedItems, long interval)
    {
        if (sortedItems.get(0).getBytes() != 0)
        {
            sortedItems.add(new BytesMsItem(0L, 0.0));
            Collections.sort(sortedItems);
        }
        int length = sortedItems.size();
        for (int i = 0; i < length-1; ++i)
        {
            BytesMsItem item0 = sortedItems.get(i);
            BytesMsItem item1 = sortedItems.get(i+1);

            if (item1.getBytes() - item0.getBytes() > interval)
            {
                double K = (item1.getMs() - item0.getMs()) / (item1.getBytes() - item0.getBytes());
                long bytes = item0.getBytes() + interval;
                while (bytes < item1.getBytes())
                {
                    long deltaBytes = bytes - item0.getBytes();
                    sortedItems.add(new BytesMsItem(bytes, item0.getMs() + deltaBytes*K));
                    bytes += interval;
                }
            }
        }
        Collections.sort(sortedItems);
    }

    /**
     * build pixels cost model by fetching metrics from Prometheus.
     * @param host
     * @param port
     * @return
     */
    public static PixelsCostModel build (String host, int port) throws CostFunctionException
    {
        String url = "http://" + host + ":" + port + "/api/v1/query?query=";
        String seekCostQuery = "avg(avg_over_time(pixels_seek_cost[1h]))by(distance,job)";
        String seqReadCostQuery = "avg(avg_over_time(pixels_seq_read_cost[1h]))by(length,job)";
        String lambdaCostQuery = "avg(avg_over_time(pixels_read_lambda_cost[1h]))by(name,job)";
        String intervalQuery = "min(min_over_time(pixels_bytesms_interval[1m]))by(job)";

        final String job = ConfigFactory.Instance().getProperty("prometheus.cluster.monitor.job");

        PixelsCostModel pixelsCostModel = null;

        try
        {
            String seekCostJson = HttpUtils.Instance().getPageContent(url + seekCostQuery);
            String seqReadCostJson = HttpUtils.Instance().getPageContent(url + seqReadCostQuery);
            String lambdaCostJson = HttpUtils.Instance().getPageContent(url + lambdaCostQuery);
            String intervalJson = HttpUtils.Instance().getPageContent(url + intervalQuery);

            SeekCostObj seekCostObj = JSON.parseObject(seekCostJson, SeekCostObj.class);
            SeqReadCostObj seqReadCostObj = JSON.parseObject(seqReadCostJson, SeqReadCostObj.class);
            LambdaCostObj lambdaCostObj = JSON.parseObject(lambdaCostJson, LambdaCostObj.class);
            IntervalObj intervalObj = JSON.parseObject(intervalJson, IntervalObj.class);

            // build interval
            long interval = 0;
            for (IntervalObj.Result result : intervalObj.getData().getResult())
            {
                if (result.getMetric().getJob().equals(job))
                {
                    interval = result.getValue().get(1).longValue();
                    break;
                }
            }

            if (interval == 0)
            {
                throw new CostFunctionException(
                        "interval == 0, this can be caused by the incorrect job name: " + job);
            }

            // build seek cost
            List<BytesMsItem> seekCostItems = new ArrayList<>();
            for (SeekCostObj.Result result : seekCostObj.getData().getResult())
            {
                if (result.getMetric().getJob().equals(job))
                {
                    seekCostItems.add(new BytesMsItem(result.getMetric().getDistance(), result.getValue().get(1)));
                }
            }
            if (seekCostItems.size() < 2)
            {
                throw new CostFunctionException("number of seek cost metrics is less than 2.");
            }
            Collections.sort(seekCostItems);
            padding(seekCostItems, interval);
            List<Coordinate> seekCostPoints = new ArrayList<>();
            for (BytesMsItem item : seekCostItems)
            {
                seekCostPoints.add(new Coordinate((long)item.getBytes(), item.getMs()));
            }

            // build sequential read cost
            List<BytesMsItem> seqReadCostItems = new ArrayList<>();
            for (SeqReadCostObj.Result result : seqReadCostObj.getData().getResult())
            {
                if (result.getMetric().getJob().equals(job))
                {
                    seqReadCostItems.add(new BytesMsItem(result.getMetric().getLength(), result.getValue().get(1)));
                }
            }
            if (seqReadCostItems.size() < 2)
            {
                throw new CostFunctionException("number of seq read cost metrics is less than 2.");
            }
            Collections.sort(seqReadCostItems);
            padding(seqReadCostItems, interval);
            List<Coordinate> seqReadCostPoints = new ArrayList<>();
            for (BytesMsItem item : seqReadCostItems)
            {
                seqReadCostPoints.add(new Coordinate((long)item.getBytes(), item.getMs()));
            }

            // build lambda cost
            LambdaCost lambdaCost = new LambdaCost();
            for (LambdaCostObj.Result result : lambdaCostObj.getData().getResult())
            {
                if (result.getMetric().getJob().equals(job))
                {
                    lambdaCost.addNamedCost(new NamedCost(result.getMetric().getName(), result.getValue().get(1)));
                }
            }

            // Happy New Year!
            // @ Donghai, 2018.2.15 21:32 除夕

            RealSeekCost seekCost = new RealSeekCost(seekCostItems.get(1).getBytes()-seekCostItems.get(0).getBytes(), seekCostPoints);
            RealSeqReadCost seqReadCost = new RealSeqReadCost(seqReadCostItems.get(1).getBytes()-seqReadCostItems.get(0).getBytes(), seqReadCostPoints);

            pixelsCostModel = new PixelsCostModel(seekCost, seqReadCost, lambdaCost);
        } catch (IOException e)
        {
            LogFactory.Instance().getLog().error("I/O error when getting metrics from prometheus.", e);
        }
        return pixelsCostModel;
    }
}
