package cn.edu.ruc.iir.rainbow.layout.cost;

public class PowerSeekCost implements SeekCost
{
    private static final double K1 = 0.005;
    private static final double TURNING_POINT = 36000000;
    private static final double K2 = 0.000000003;
    private static final double B = 29.696969697;

    @Override
    public double calculate(double distance)
    {
        return distance <= TURNING_POINT ? Math.sqrt(distance) * K1 : (distance * K2 + B);
    }

}
