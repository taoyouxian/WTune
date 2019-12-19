package cn.edu.ruc.iir.rainbow.server;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.exception.AlgoException;
import cn.edu.ruc.iir.rainbow.common.exception.ColumnNotFoundException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.layout.algorithm.Algorithm;
import cn.edu.ruc.iir.rainbow.layout.algorithm.AlgorithmFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.impl.tune.FastScoaTune;
import cn.edu.ruc.iir.rainbow.layout.builder.ColumnOrderBuilder;
import cn.edu.ruc.iir.rainbow.layout.builder.RealSeekCostBuilder;
import cn.edu.ruc.iir.rainbow.layout.builder.WorkloadBuilder;
import cn.edu.ruc.iir.rainbow.layout.cost.LinearSeekCost;
import cn.edu.ruc.iir.rainbow.layout.cost.PowerSeekCost;
import cn.edu.ruc.iir.rainbow.layout.cost.SeekCost;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.server
 * @ClassName: DQNService
 * @Description:
 * @author: tao
 * @date: Create in 2019-12-18 20:59
 **/
@Service
public class DQNService {
    private String tunePath;
    private Properties params;
    private Properties results;
    private Algorithm algo;

    @PostConstruct
    private void initDQN() {
        tunePath = ConfigFactory.Instance().getProperty("tune.path");

        params = new Properties();
        params.setProperty("algorithm.name", "scoa.tune");
        params.setProperty("schema.file", tunePath + "default/schema.txt");
        params.setProperty("workload.file", tunePath + "default/workload.txt");
        params.setProperty("ordered.schema.file", tunePath + "default/ordered_schema.txt");
        params.setProperty("seek.cost.function", "power");
        params.setProperty("computation.budget", "100");
        params.setProperty("num.row.group", "100");
        params.setProperty("row.group.size", "134217728");

        results = new Properties(params);
        results.setProperty("success", "false");
        execute(params);
    }

    private void execute(Properties params) {
        String algoName = params.getProperty("algorithm.name", "scoa.tune").toLowerCase();
        String schemaFilePath = params.getProperty("schema.file", tunePath + "default/schema.txt");
        String workloadFilePath = params.getProperty("workload.file", tunePath + "default/workload.txt");
//        String orderedFilePath = params.getProperty("ordered.schema.file");
        long budget = Long.parseLong(params.getProperty("computation.budget", "200"));
        // TODO deal with possible IllegalArgumentException and NullPointerException thrown by `Enum.valueOf()`
        SeekCost.Type funcType = SeekCost.Type.valueOf(
                params.getProperty("seek.cost.function", SeekCost.Type.POWER.name()).toUpperCase());
        SeekCost seekCostFunction = null;

        if (funcType == SeekCost.Type.LINEAR) {
            seekCostFunction = new LinearSeekCost();
        } else if (funcType == SeekCost.Type.POWER) {
            seekCostFunction = new PowerSeekCost();
        } else if (funcType == SeekCost.Type.SIMULATED) {
            try {
                String seekCostFilePath = params.getProperty("seek.cost.file");
                seekCostFunction = RealSeekCostBuilder.build(new File(seekCostFilePath));
            } catch (IOException e) {
                ExceptionHandler.Instance().log(ExceptionType.ERROR,
                        "get seek cost file error", e);
                return;
            }
        }

        try {
            List<Column> initColumnOrder = ColumnOrderBuilder.build(new File(schemaFilePath));
            List<Query> workload = WorkloadBuilder.build(new File(workloadFilePath), initColumnOrder);
            algo = AlgorithmFactory.Instance().getAlgorithm(algoName,
                    budget, new ArrayList<>(initColumnOrder), workload, seekCostFunction);

            if (algo instanceof FastScoaTune) {
                FastScoaTune gs = (FastScoaTune) algo;
                gs.setNumRowGroups(Integer.parseInt(params.getProperty("num.row.group")));
                gs.setRowGroupSize(Long.parseLong(params.getProperty("row.group.size")));
                gs.setNumMapSlots(Integer.parseInt(ConfigFactory.Instance().getProperty("node.map.slots")));
                gs.setTotalMemory(Long.parseLong(ConfigFactory.Instance().getProperty("node.memory")));
                gs.setTaskInitMs(Integer.parseInt(ConfigFactory.Instance().getProperty("node.task.init.ms")));
                results.setProperty("init.cost", String.valueOf(gs.getSchemaOverhead()));
            }
            else {
                results.setProperty("init.cost", String.valueOf(algo.getSchemaSeekCost()));
            }

            // setup params of FastScoaTune
            algo.setup();
            // get best row group size
            algo.runAlgorithm();

        } catch (IOException e) {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "I/O error, check the file paths", e);
        } catch (ColumnNotFoundException e) {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "column not fount when building workload", e);
        } catch (ClassNotFoundException e) {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "algorithm class not fount", e);
        } catch (AlgoException e) {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "algorithm initialization error", e);
        }

    }

    double run(int cx, int cy) {
        double neighbourSeekCost = -1;
        if (algo instanceof FastScoaTune) {
            FastScoaTune gs = (FastScoaTune) algo;
            neighbourSeekCost = gs.getRandSeekCost(cx, cy);
        }
        else {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "algorithm class not fount", new Exception("algorithm error"));
        }

        return neighbourSeekCost;
    }

}
