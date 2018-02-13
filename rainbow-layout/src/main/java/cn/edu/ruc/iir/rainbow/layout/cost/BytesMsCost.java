package cn.edu.ruc.iir.rainbow.layout.cost;

public interface BytesMsCost
{
    enum Type
    {
        LINEAR,
        POWER,
        SIMULATED
    }

    double calculate(double bytes);
}
