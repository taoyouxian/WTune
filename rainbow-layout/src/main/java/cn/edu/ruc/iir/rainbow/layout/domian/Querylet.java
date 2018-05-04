package cn.edu.ruc.iir.rainbow.layout.domian;

public class Querylet extends Query
{
    private int originId = 0;

    public Querylet(int id, String sid, double weight)
    {
        super(id, sid, weight);
        this.originId = id;
    }

    public int getOriginId()
    {
        return originId;
    }
}
