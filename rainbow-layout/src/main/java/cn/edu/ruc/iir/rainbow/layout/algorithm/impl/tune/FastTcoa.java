package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.tune;

import cn.edu.ruc.iir.rainbow.common.exception.AlgoException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord.FastScoaGS;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.layout.algorithm.impl.tune
 * @ClassName: FastTcoa
 * @Description:
 * @author: tao
 * @date: Create in 2019-12-18 19:57
 **/
public class FastTcoa extends FastScoaGS {

    private double initSeekCost;

    public double getInitSeekCost() {
        return initSeekCost;
    }

    public double getCurrentSeekCost() {
        return currentEnergy;
    }

    @Override
    public void setup()
    {
        this.setColumnOrder(new ArrayList<>(this.getSchema()));
        //make initial columnId-columnIndex map
        Map<Integer, Integer> cidToCIdxMap = new HashMap<>();
        for (int i = 0; i < this.getColumnOrder().size(); i ++)
        {
            cidToCIdxMap.put(this.getColumnOrder().get(i).getId(), i);
        }

        //build initial query's accessed column index sets.
        for (int i = 0; i < this.getWorkload().size(); i ++)
        {
            Query curQuery = this.getWorkload().get(i);
            queryAccessedPos.add(new TreeSet<Integer>());
            for (int colIds : curQuery.getColumnIds())
            {
                // add the column indexes to query's tree set.
                queryAccessedPos.get(i).add(cidToCIdxMap.get(colIds));
            }
        }
    }

    @Override
    public void runAlgorithm() {
        try
        {
            long bestRowGroupSize = RowGroupSize.BestRowGroupSize(this.getNumMapSlots(),
                    this.getTotalMemory());

            while (this.getRowGroupSize() > bestRowGroupSize)
            {
                this.decreaseRowGroupSize();
            }
            while (this.getRowGroupSize() < bestRowGroupSize)
            {
                this.increaseRowGroupSize();
            }
            this.currentEnergy = super.getCurrentWorkloadSeekCost();
            this.initSeekCost = this.currentEnergy;
        } catch (AlgoException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "algorithm error when running fastscoa tune with group size optimization.", e);
        }
    }

    public double getRandSeekCost(int i, int j)
    {
        //calculate new cost
        this.currentEnergy = getNeighbourSeekCost(i, j);
        //try to accept it
        updateColumnOrder(i, j);
        return this.currentEnergy;
    }

}
