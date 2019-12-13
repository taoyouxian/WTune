package cn.edu.ruc.iir.rainbow.layout.cost;

public class PowerSeekCost implements SeekCost
{
    private static final double K1 = 0.005;
    private static final double TURNING_POINT = 36_000_000;
    private static final double K2 = 0.000000003;
    private static final double B = 29.696969697;

    private static final long SegmentSize = 128l*1024l*1024l;
    private static final double Const = 30;

    /**
     * Calculate the cost of an I/O operation (typically read or seek).
     * Offset is not considered.
     * @param distance the bytes to be seek.
     * @return
     */
    @Override
    public double calculate(double distance)
    {
        return distance <= TURNING_POINT ? Math.sqrt(distance) * K1 : (distance * K2 + B);
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
        // TODO: implement segmented seek cost calculation.
        long from = fromByte;
        long to = from + (long)distance;
        if (from/SegmentSize < to/SegmentSize)
        {
            return Const;
        }
        else
        {
            return calculate(distance);
        }
    }

}
