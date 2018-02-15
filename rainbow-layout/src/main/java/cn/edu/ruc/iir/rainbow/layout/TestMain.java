package cn.edu.ruc.iir.rainbow.layout;

import cn.edu.ruc.iir.rainbow.common.exception.CostFunctionException;
import cn.edu.ruc.iir.rainbow.layout.builder.PixelsCostModelBuilder;
import cn.edu.ruc.iir.rainbow.layout.cost.PixelsCostModel;

public class TestMain
{
    public static void main(String[] args)
    {
        try
        {
            PixelsCostModel pixelsCostModel = PixelsCostModelBuilder.build("10.77.40.241", 9090);
            System.out.println(pixelsCostModel);
            System.out.println(pixelsCostModel.getSeekCost().calculate(100000));
            System.out.println(pixelsCostModel.getSeqReadCost().calculate(200000));
            System.out.println(pixelsCostModel.getLambdaCost().calculate());
        } catch (CostFunctionException e)
        {
            e.printStackTrace();
        }
    }
}
