package cn.edu.ruc.iir.rainbow.layout.builder;

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
     * this method is not tested.
     * @param host
     * @param port
     * @return
     */
    public static PixelsCostModel build (String host, int port) throws CostFunctionException
    {
        String url = "http://" + host + ":" + port + "/api/v1/query?query=";
        String seekCostQuery = "avg(pixels_seek_cost)by(distance)";
        String seqReadCostQuery = "avg(pixels_seq_read_cost)by(length)";
        String lambdaCostQuery = "avg(pixels_read_lambda_cost)by(name)";

        PixelsCostModel pixelsCostModel = null;

        try
        {
            String seekCostJson = HttpUtils.Instance().getPageContent(url + seekCostQuery);
            String seqReadCostJson = HttpUtils.Instance().getPageContent(url + seqReadCostQuery);
            String lambdaCostJson = HttpUtils.Instance().getPageContent(url + lambdaCostQuery);

            SeekCostObj seekCostObj = JSON.parseObject(seekCostJson, SeekCostObj.class);
            SeqReadCostObj seqReadCostObj = JSON.parseObject(seqReadCostJson, SeqReadCostObj.class);
            LambdaCostObj lambdaCostObj = JSON.parseObject(lambdaCostJson, LambdaCostObj.class);

            List<BytesMsItem> seekCostItems = new ArrayList<>();
            for (SeekCostObj.Result result : seekCostObj.getData().getResult())
            {
                seekCostItems.add(new BytesMsItem(result.getMetric().getDistance(), result.getValue().get(1)));
            }
            if (seekCostItems.size() < 2)
            {
                throw new CostFunctionException("number of seek cost metrics is less than 2.");
            }
            Collections.sort(seekCostItems);
            List<Coordinate> seekCostPoints = new ArrayList<>();
            for (BytesMsItem item : seekCostItems)
            {
                seekCostPoints.add(new Coordinate((long)item.getBytes(), item.getMs()));
            }

            List<BytesMsItem> seqReadCostItems = new ArrayList<>();
            for (SeqReadCostObj.Result result : seqReadCostObj.getData().getResult())
            {
                seqReadCostItems.add(new BytesMsItem(result.getMetric().getLength(), result.getValue().get(1)));
            }
            if (seqReadCostItems.size() < 2)
            {
                throw new CostFunctionException("number of seq read cost metrics is less than 2.");
            }
            Collections.sort(seqReadCostItems);
            List<Coordinate> seqReadCostPoints = new ArrayList<>();
            for (BytesMsItem item : seqReadCostItems)
            {
                seqReadCostPoints.add(new Coordinate((long)item.getBytes(), item.getMs()));
            }

            LambdaCost lambdaCost = new LambdaCost();
            for (LambdaCostObj.Result result : lambdaCostObj.getData().getResult())
            {
                lambdaCost.addNamedCost(new NamedCost(result.getMetric().getName(), result.getValue().get(1)));
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
