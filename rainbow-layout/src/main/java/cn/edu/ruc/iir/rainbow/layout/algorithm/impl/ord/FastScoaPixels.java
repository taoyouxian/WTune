package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord;

import cn.edu.ruc.iir.rainbow.layout.cost.SeqReadCost;

/**
 * column ordering and query-wise split size optimization for Pixels
 */
public class FastScoaPixels extends FastScoa
{
    private SeqReadCost seqReadCostFunction = null;
    private double lambdaCost = 0.0;

    @Override
    public void setup()
    {
        // the initial column order and workload are given;
        // read #row group inside a block from configuration;
        // build pixels cost model from prometheus;
        // choose the best split size for each query;
        // rebuild schema, currentColumnOrder and workload.
        super.setup();
    }

}
