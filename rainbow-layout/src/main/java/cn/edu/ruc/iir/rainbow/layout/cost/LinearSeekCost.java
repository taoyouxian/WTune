package cn.edu.ruc.iir.rainbow.layout.cost;

public class LinearSeekCost implements SeekCost
{
    private static final double K = 0.005;

    @Override
    public double calculate(double distance)
    {
        return distance * K;
    }

    /**
     * Calculate the cost of a seek operation.
     *
     * @param fromByte the offset in bytes from which the seek will be performed.
     * @param distance    the bytes to be seek.
     * @return
     */
    @Override
    public double calculate(long fromByte, double distance)
    {
        return calculate(distance);
    }

}
