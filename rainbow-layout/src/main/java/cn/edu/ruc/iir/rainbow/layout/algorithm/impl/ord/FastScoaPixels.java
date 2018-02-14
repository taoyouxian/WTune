package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord;

/**
 * column ordering and query-wise split size optimization for Pixels
 */
public class FastScoaPixels extends FastScoa
{
    // given the initial column order, workload, and #row group inside a block,
    // first, choose the best split size for each query;
    // second, flat the workload and block (with a set of row groups);
    // third, run column ordering, output split size and pixels layout.

}
