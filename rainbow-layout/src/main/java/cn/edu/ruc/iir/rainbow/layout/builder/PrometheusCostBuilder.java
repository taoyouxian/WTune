package cn.edu.ruc.iir.rainbow.layout.builder;

import cn.edu.ruc.iir.rainbow.common.HttpUtils;
import cn.edu.ruc.iir.rainbow.layout.cost.RealSeqReadCost;

import java.io.IOException;

public class PrometheusCostBuilder
{
    private PrometheusCostBuilder() {}

    /**
     * this method is not tested.
     * @param host
     * @param port
     * @return
     */
    public static RealSeqReadCost build (String host, int port)
    {
        String url = "http://" + host + ":" + port + "/api/v1/query?query=";
        String seekCostQuery = "avg(pixels_seek_cost)by(distance)";
        String seqReadCostQuery = "avg(pixels_seq_read_cost)by(length)";
        String lambdaCostQuery = "avg(pixels_read_lambda_cost)by(name)";
        try
        {
            String seekCostJson = HttpUtils.Instance().getPageContent(url + seekCostQuery);
            String seqReadCostJson = HttpUtils.Instance().getPageContent(url + seqReadCostQuery);
            String lambdaCostJson = HttpUtils.Instance().getPageContent(url + lambdaCostQuery);
            System.out.println(seekCostJson);
            System.out.println(seqReadCostJson);
            System.out.println(lambdaCostJson);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args)
    {
        build("10.77.40.241", 9090);
    }
}
