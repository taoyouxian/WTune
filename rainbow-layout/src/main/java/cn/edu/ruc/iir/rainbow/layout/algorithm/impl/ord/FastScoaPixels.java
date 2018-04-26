package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.LogFactory;
import cn.edu.ruc.iir.rainbow.common.exception.ConfigrationException;
import cn.edu.ruc.iir.rainbow.common.exception.CostFunctionException;
import cn.edu.ruc.iir.rainbow.layout.builder.PixelsCostModelBuilder;
import cn.edu.ruc.iir.rainbow.layout.cost.PixelsCostModel;
import cn.edu.ruc.iir.rainbow.layout.cost.PowerSeekCost;
import cn.edu.ruc.iir.rainbow.layout.cost.SeqReadCost;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Columnlet;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;

import java.util.ArrayList;
import java.util.List;

/**
 * column ordering and query-wise split size optimization for Pixels
 */
public class FastScoaPixels extends FastScoa
{
    private SeqReadCost seqReadCostFunction = null;
    private double lambdaCost = 0.0;
    private int numRowGroupPerBlock = 0;
    private boolean isSetup = false;

    public FastScoaPixels ()
    {
        // read #row group inside a block from configuration
        String strNumRowGroup = ConfigFactory.Instance().getProperty("pixels.num.row.group.perblock");
        if (strNumRowGroup != null)
        {
            this.numRowGroupPerBlock = Integer.parseInt(strNumRowGroup);
        }
        else
        {
            LogFactory.Instance().getLog().error("FastScoaPixels configuration error",
                    new ConfigrationException("pixels.row.group.num is not a valid number."));
        }

        String promHost = ConfigFactory.Instance().getProperty("prometheus.host");
        int promPort = 0;
        String strPromPort = ConfigFactory.Instance().getProperty("prometheus.port");
        if (strPromPort != null)
        {
            promPort = Integer.parseInt(strPromPort);
        }
        else
        {
            LogFactory.Instance().getLog().error("FastScoaPixels configuration error",
                    new ConfigrationException("prometheus port is not a valid number."));
        }

        // build pixels cost model from prometheus
        try
        {
            PixelsCostModel costModel = PixelsCostModelBuilder.build(promHost, promPort);
            this.setSeekCostFunction(new PowerSeekCost());
            this.setSeqReadCostFunction(costModel.getSeqReadCost());
            this.setLambdaCost(costModel.getLambdaCost().calculate());
        } catch (CostFunctionException e)
        {
            LogFactory.Instance().getLog().error("FastScoaPixels build prometheus cost error.", e);
        }
    }

    @Override
    public void setup ()
    {
        // the initial column order and workload are given;

        // choose the best split size for each query;
        long memSizeBytes = 0;
        String strMemSizeBytes = ConfigFactory.Instance().getProperty("node.memory");
        if (strMemSizeBytes != null)
        {
            memSizeBytes = Long.parseLong(strMemSizeBytes);
        }
        else
        {
            LogFactory.Instance().getLog().error("FastScoaPixels configuration error",
                    new ConfigrationException("node.memory is not a valid number."));
        }
        int mapSlots = 0;
        String strMapSlots = ConfigFactory.Instance().getProperty("node.map.slots");
        if (strMapSlots != null)
        {
            mapSlots = Integer.parseInt(strMapSlots);
        }
        else
        {
            LogFactory.Instance().getLog().error("FastScoaPixels configuration error",
                    new ConfigrationException("node.map.slots is not a valid number."));
        }
        double memAmp = 0.0;
        String strMemAmp = ConfigFactory.Instance().getProperty("pixels.memory.amp");
        if (strMemAmp != null)
        {
            memAmp = Double.parseDouble(strMemAmp);
        }
        else
        {
            LogFactory.Instance().getLog().error("FastScoaPixels configuration error",
                    new ConfigrationException("pixels.memory.amp is not a valid number."));
        }
        double lambdaThreshold = 0.0;
        String strLambdaThreshold = ConfigFactory.Instance().getProperty("pixels.lambda.threshold");
        if (strLambdaThreshold != null)
        {
            lambdaThreshold = Double.parseDouble(strLambdaThreshold);
        }
        else
        {
            LogFactory.Instance().getLog().error("FastScoaPixels configuration error",
                    new ConfigrationException("pixels.delta.threshold is not a valid number."));
        }

        int numBlockPerNode = 0;
        String strNumBlock = ConfigFactory.Instance().getProperty("pixels.num.block.pernode");
        if (strNumBlock != null)
        {
            numBlockPerNode = Integer.parseInt(strNumBlock);
        }
        else
        {
            LogFactory.Instance().getLog().error("FastScoaPixels configuration error",
                    new ConfigrationException("pixels.num.block.pernode is not a valid number."));
        }

        int mapWaves = 0;
        String strMapWaves = ConfigFactory.Instance().getProperty("node.map.waves");
        if (strMapWaves != null)
        {
            mapWaves = Integer.parseInt(strMapWaves);
        }
        else
        {
            LogFactory.Instance().getLog().error("FastScoaPixels configuration error",
                    new ConfigrationException("node.map.waves is not a valid number."));
        }

        int numColumns = this.getSchema().size();

        /*double rowGroupSize = 0;
        for (Column column : this.getSchema())
        {
            rowGroupSize += column.getSize();
        }*/

        List<Query> rebuiltWorklod = new ArrayList<>();
        for (Query query : this.getWorkload())
        {
            double size = 0;
            for (Column column : this.getSchema())
            {
                if (query.hasColumnId(column.getId()))
                {
                    size += column.getSize();
                }
            }
            //double seekCost = this.getQuerySeekCost(this.getSchema(), query);
            double seqReadCost = this.seqReadCostFunction.calculate(size);
            //System.out.println(seekCost + ", " + seqReadCost);

            /**
             * while loop #1
             * this loop is equivalent to while loop #2
            int tmpMapSlots = mapSlots;
            while (tmpMapSlots > 1 && numBlockPerNode*numRowGroupPerBlock*seqReadCost/tmpMapSlots/mapWaves < lambdaCost)
            {
                tmpMapSlots >>= 1;
            }
             */

            /**
             * max split size if calculated by the limitation of memory and degree of parallelism (mapSlots).
             */
            int maxSplitSize = floor2n((int)(memSizeBytes / memAmp / mapSlots / size));
            if (maxSplitSize > numBlockPerNode*numRowGroupPerBlock/mapSlots/mapWaves)
            {
                maxSplitSize = numBlockPerNode*numRowGroupPerBlock/mapSlots/mapWaves;
            }
            int splitSize = 1;
            /**
             * by this loop, we ensure the proportion of lambda cost is lower than the given threshold.
             */
            while (lambdaCost/splitSize/(seqReadCost) > lambdaThreshold)
            {
                if (splitSize << 1 <= maxSplitSize)
                {
                    splitSize <<= 1;
                }
                else
                {
                    break;
                }
            }

            /**
             * this loop is not correct.
             * we should always use the max map slots.
             */
            //if (maxSplitSize == splitSize)
            //{
                /**
                 * while loop #2
                 * we use the seqReadCost to estimate the CPU cost of processing a row group.
                 * by the following while loop, we ensure the map slot is not set too high.
                 * TODO: it is better to collect task CPU cost through Prometheus.
                 */
            //    while (splitSize < Integer.MAX_VALUE && splitSize * seqReadCost < lambdaCost)
            //    {
            //        splitSize <<= 2;
            //    }
            //}

            //System.out.println(maxSplitSize + ", " + splitSize + ", " + (splitSize*size/1024/1024));

            // rebuild the workload
            if (splitSize < numRowGroupPerBlock)
            {
                int numSplits = numRowGroupPerBlock/splitSize;
                for (int splitId = 0; splitId < numSplits; ++splitId)
                {
                    int rowgroupIdBase = splitId*splitSize;
                    Query rebuiltQuery = new Query(query.getId(), query.getSid(), query.getWeight());
                    for (int i = 0; i < splitSize; ++i)
                    {
                        int rowGroupId = rowgroupIdBase + i;
                        for (int columnId : query.getColumnIds())
                        {
                            rebuiltQuery.addColumnId(rowGroupId*numColumns+columnId);
                        }
                    }
                    rebuiltWorklod.add(rebuiltQuery);
                }
            }
            else
            {
                Query rebuiltQuery = new Query(query.getId(), query.getSid(), query.getWeight());
                for (int rowGroupId = 0; rowGroupId < numRowGroupPerBlock; ++rowGroupId)
                {
                    for (int columnId : query.getColumnIds())
                    {
                        rebuiltQuery.addColumnId(rowGroupId*numColumns+columnId);
                    }
                }
                rebuiltWorklod.add(rebuiltQuery);
            }
        }

        this.setWorkload(rebuiltWorklod);

        // rebuild schema
        List<Column> rebuiltSchema = new ArrayList<>();
        // by sequentially duplicate the columnlet, we can generally start from a very good point.
        for (Column column : this.getSchema())
        {
            for (int rowGroupId = 0; rowGroupId < numRowGroupPerBlock; ++rowGroupId)
            {
                Columnlet columnlet = new Columnlet(rowGroupId, numColumns, column);
                rebuiltSchema.add(columnlet);
            }
        }
        this.setSchema(rebuiltSchema);

        super.setup();

        String strCoolingRate = ConfigFactory.Instance().getProperty("scoa.pixels.cooling_rate");
        String strInitTemp = ConfigFactory.Instance().getProperty("scoa.pixels.init.temperature");
        if (strCoolingRate != null)
        {
            this.coolingRate = Double.parseDouble(strCoolingRate);
        }
        if (strInitTemp != null)
        {
            this.temperature = Double.parseDouble(strInitTemp);
        }

        this.isSetup = true;
    }

    /**
     * get the floor value 2^n of i. for example, if i=9, floor2n(i)=2^3=8
     * @param i
     * @return
     */
    private static int floor2n (int i)
    {
        int res = 0;
        for (int n = 0; n < 31 && 1<<n <= i; ++n)
        {
            res = 1<<n;
        }
        return res;
    }

    public SeqReadCost getSeqReadCostFunction()
    {
        return seqReadCostFunction;
    }

    protected void setSeqReadCostFunction(SeqReadCost seqReadCostFunction)
    {
        this.seqReadCostFunction = seqReadCostFunction;
    }

    public double getLambdaCost()
    {
        return lambdaCost;
    }

    protected void setLambdaCost(double lambdaCost)
    {
        this.lambdaCost = lambdaCost;
    }

    /**
     * get the seek cost of the whole workload (on the current column order).
     *
     * @return
     */
    @Override
    public double getCurrentWorkloadSeekCost()
    {
        if (this.isSetup)
        {
            return super.getCurrentWorkloadSeekCost();
        }
        else
        {
            return this.numRowGroupPerBlock * super.getCurrentWorkloadSeekCost();
        }
    }

    @Override
    public double getSchemaSeekCost()
    {
        if (this.isSetup)
        {
            return super.getSchemaSeekCost();
        }
        else
        {
            return this.numRowGroupPerBlock * super.getSchemaSeekCost();
        }
    }

    /**
     * get the seek cost of a query (on the given column order).
     * this is a general function, sub classes can override it.
     *
     * @param columnOrder
     * @param query
     * @return
     */
    @Override
    protected double getQuerySeekCost(List<Column> columnOrder, Query query)
    {
        double querySeekCost = 0, seekDistance = 0;
        int accessedColumnNum = 0;
        boolean finishFirstRead = false;
        for (int i = 0; i < columnOrder.size(); ++i)
        {
            if (query.getColumnIds().contains(columnOrder.get(i).getId()))
            {
                // column i has been accessed by the query
                if (finishFirstRead == false)
                {
                    finishFirstRead = true;
                }
                else
                {
                    querySeekCost += this.getSeekCostFunction().calculate(seekDistance);
                }
                seekDistance = 0;
                ++accessedColumnNum;

                if (accessedColumnNum >= query.getColumnIds().size())
                {
                    // the query has accessed all the necessary columns
                    break;
                }
            } else
            {
                if (finishFirstRead == true)
                {
                    // column i has been skipped (seek over) by the query
                    seekDistance += columnOrder.get(i).getSize();
                }
            }
        }
        return querySeekCost;
    }
}
