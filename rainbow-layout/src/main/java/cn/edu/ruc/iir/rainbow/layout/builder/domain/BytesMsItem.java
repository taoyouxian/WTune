package cn.edu.ruc.iir.rainbow.layout.builder.domain;

public class BytesMsItem implements Comparable<BytesMsItem>
{
    private long bytes;
    private double ms;

    public BytesMsItem(long bytes, double ms)
    {
        this.bytes = bytes;
        this.ms = ms;
    }

    public long getBytes()
    {
        return bytes;
    }

    public void setBytes(long bytes)
    {
        this.bytes = bytes;
    }

    public double getMs()
    {
        return ms;
    }

    public void setMs(double ms)
    {
        this.ms = ms;
    }

    @Override
    public int compareTo(BytesMsItem bytesMsItem)
    {
        if (this.bytes < bytesMsItem.bytes)
        {
            return -1;
        }
        else if (this.bytes > bytesMsItem.bytes)
        {
            return 1;
        }
        return 0;
    }
}
