package cn.edu.ruc.iir.rainbow.layout.builder.domain;

import java.util.ArrayList;
import java.util.List;

public class SplitStrategyObj
{
    private int numRowGroupInBlock;
    private List<SplitPatternObj> splitPatterns = new ArrayList<>();

    public int getNumRowGroupInBlock()
    {
        return numRowGroupInBlock;
    }

    public void setNumRowGroupInBlock(int numRowGroupInBlock)
    {
        this.numRowGroupInBlock = numRowGroupInBlock;
    }

    public List<SplitPatternObj> getSplitPatterns()
    {
        return splitPatterns;
    }

    public void setSplitPatterns(List<SplitPatternObj> splitPatterns)
    {
        this.splitPatterns = splitPatterns;
    }

    public void addSplitPatterns(SplitPatternObj splitPattern)
    {
        this.splitPatterns.add(splitPattern);
    }
}
