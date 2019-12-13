package cn.edu.ruc.iir.rainbow.layout.cost;

public class PixelsCostModel
{
    private RealSeekCost seekCost;
    private RealSeqReadCost seqReadCost;
    private LambdaCost lambdaCost;

    public PixelsCostModel(RealSeekCost seekCost, RealSeqReadCost seqReadCost, LambdaCost lambdaCost)
    {
        this.seekCost = seekCost;
        this.seqReadCost = seqReadCost;
        this.lambdaCost = lambdaCost;
    }

    public void setSeekCost(RealSeekCost seekCost)
    {
        this.seekCost = seekCost;
    }

    public void setSeqReadCost(RealSeqReadCost seqReadCost)
    {
        this.seqReadCost = seqReadCost;
    }

    public void setLambdaCost(LambdaCost lambdaCost)
    {
        this.lambdaCost = lambdaCost;
    }

    public RealSeekCost getSeekCost()
    {
        return seekCost;
    }

    public RealSeqReadCost getSeqReadCost()
    {
        return seqReadCost;
    }

    public LambdaCost getLambdaCost()
    {
        return lambdaCost;
    }
}
