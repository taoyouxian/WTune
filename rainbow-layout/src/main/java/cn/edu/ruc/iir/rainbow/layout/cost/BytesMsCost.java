package cn.edu.ruc.iir.rainbow.layout.cost;

/**
 * @Author hank
 * @Date 2018-02-01
 */
public interface BytesMsCost
{
    enum Type
    {
        LINEAR,
        POWER,
        SIMULATED
    }

    /**
     * Calculate the cost of an I/O operation (typically read or seek).
     * Offset is not considered.
     * @param bytes the bytes to be read or seek.
     * @return
     */
    double calculate(double bytes);

    /**
     * Calculate the cost of an I/O operation (typically read or seek).
     * @param fromByte the offset in bytes from which the operation will be performed.
     * @param bytes the bytes to be read or seek.
     * @return
     */
    double calculate(long fromByte, double bytes);
}
