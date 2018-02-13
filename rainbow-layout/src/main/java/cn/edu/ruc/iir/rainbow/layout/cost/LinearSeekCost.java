package cn.edu.ruc.iir.rainbow.layout.cost;

public class LinearSeekCost implements SeekCost
{
    private static final double K = 0.005;

    @Override
    public double calculate(double distance)
    {
        return distance * K;
    }

}
