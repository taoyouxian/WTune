package cn.edu.ruc.iir.rainbow.layout.algorithm;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.LogFactory;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.rainbow.layout.cost.SeekCost;
import cn.edu.ruc.iir.rainbow.common.exception.AlgoException;
import org.apache.commons.logging.Log;

import java.util.List;

public class AlgorithmFactory
{
    private static AlgorithmFactory instance = null;

    private AlgorithmFactory()
    {

    }

    public static AlgorithmFactory Instance()
    {
        if (instance == null)
        {
            instance = new AlgorithmFactory();
        }
        return instance;
    }

    private Log log = LogFactory.Instance().getLog();

    public Algorithm getAlgorithm(String algoName,
                                  long computationBudget,
                                  List<Column> initColumnOrder,
                                  List<Query> workload,
                                  SeekCost seekCostFunction) throws ClassNotFoundException, AlgoException
    {
        String className = ConfigFactory.Instance().getProperty(algoName);
        Class<?> algoClass = Class.forName(className);
        Algorithm algo = null;
        try
        {
            algo = (Algorithm) algoClass.newInstance();
            algo.setComputationBudget(computationBudget);
            algo.setSchema(initColumnOrder);
            algo.setWorkload(workload);
            if (seekCostFunction != null)
            {
                algo.setSeekCostFunction(seekCostFunction);
            }
        } catch (Exception e)
        {
            log.error("algorithm construction error: ", e);
            throw new AlgoException("algo class does not have a non-param constructor.");
        }

        return algo;
    }

    /**
     * Build the algorithm instance without providing seek cost function.
     * In this case, the algorithm should build cost model by itself.
     * @param algoName
     * @param computationBudget
     * @param initColumnOrder
     * @param workload
     * @return
     * @throws ClassNotFoundException
     * @throws AlgoException
     */
    public Algorithm getAlgorithm(String algoName,
                                  long computationBudget,
                                  List<Column> initColumnOrder,
                                  List<Query> workload) throws ClassNotFoundException, AlgoException
    {
        return this.getAlgorithm(algoName, computationBudget, initColumnOrder, workload, null);
    }
}
