package cn.edu.ruc.iir.rainbow.server;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.exception.AlgoException;
import cn.edu.ruc.iir.rainbow.common.exception.ColumnNotFoundException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.layout.algorithm.Algorithm;
import cn.edu.ruc.iir.rainbow.layout.algorithm.AlgorithmFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.impl.tune.FastScoaGSLog;
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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.server
 * @ClassName: ScoaService
 * @Description:
 * @author: tao
 * @date: Create in 2019-12-20 12:39
 **/
@Service
public class ScoaService {
    private String tunePath;
    private Properties params;
    private Properties results;
    private Algorithm algo;
    private boolean scoaInit;

    @PostConstruct
    private void initScoaParams() {
        tunePath = ConfigFactory.Instance().getProperty("tune.path");

        params = new Properties();
        params.setProperty("algorithm.name", "scoa.gs.log");
        params.setProperty("schema.file", tunePath + "default/schema.txt");
        params.setProperty("workload.file", tunePath + "default/workload.txt");
        params.setProperty("ordered.schema.file", tunePath + "default/ordered_schema.txt");
        params.setProperty("seek.cost.function", "power");
        params.setProperty("computation.budget", "100");
        params.setProperty("num.row.group", "100");
        params.setProperty("row.group.size", "134217728");
        params.setProperty("log.file", tunePath + "default/scoa_log.txt");

        results = new Properties(params);
        results.setProperty("success", "false");
    }

    public void setScoaInit(boolean scoaInit) {
        this.scoaInit = scoaInit;
        execute(params);
    }

    public void execute(Properties params) {
        String algoName = params.getProperty("algorithm.name", "scoa.gs.log").toLowerCase();
        String schemaFilePath = params.getProperty("schema.file", tunePath + "default/schema.txt");
        String workloadFilePath = params.getProperty("workload.file", tunePath + "default/workload.txt");
        String orderedFilePath = params.getProperty("ordered.schema.file");
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

            if (algo instanceof FastScoaGSLog) {
                FastScoaGSLog gs = (FastScoaGSLog) algo;
                gs.setNumRowGroups(Integer.parseInt(params.getProperty("num.row.group")));
                gs.setRowGroupSize(Long.parseLong(params.getProperty("row.group.size")));
                gs.setNumMapSlots(Integer.parseInt(ConfigFactory.Instance().getProperty("node.map.slots")));
                gs.setTotalMemory(Long.parseLong(ConfigFactory.Instance().getProperty("node.memory")));
                gs.setTaskInitMs(Integer.parseInt(ConfigFactory.Instance().getProperty("node.task.init.ms")));
                results.setProperty("init.cost", String.valueOf(gs.getSchemaOverhead()));
                // setup params of FastScoaGSLog
                gs.setup(params);
            } else {
                results.setProperty("init.cost", String.valueOf(algo.getSchemaSeekCost()));
            }

            // get best row group size
            if (this.scoaInit) {
                algo.runAlgorithm();
                algo.cleanup();
            }

            if (algo instanceof FastScoaGSLog) {
                FastScoaGSLog gs = (FastScoaGSLog) algo;
                results.setProperty("final.cost", String.valueOf(gs.getCurrentOverhead()));
                results.setProperty("row.group.size", String.valueOf(gs.getRowGroupSize()));
                results.setProperty("num.row.group", String.valueOf(gs.getNumRowGroups()));
                if (this.scoaInit) {
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(orderedFilePath + ".scoa"))) {
                        writer.write("row.group.size=" + gs.getNumRowGroups());
                        writer.newLine();
                        writer.write("init.cost=" + gs.getRowGroupSize());
                        writer.newLine();
                        writer.write("scoa.cost=" + gs.getCurrentOverhead());
                        writer.newLine();
                        writer.write("init.seek.cost=" + gs.getInitSeekCost());
                        writer.newLine();
                        writer.write("scoa.seek.cost=" + gs.getScoaSeekCost());
                    }
                }
            } else {
                results.setProperty("scoa.seek.cost", String.valueOf(algo.getCurrentWorkloadSeekCost()));
            }
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

    public double run() {
        double currentSeekCost = -1;
        if (algo instanceof FastScoaGSLog) {
            FastScoaGSLog gs = (FastScoaGSLog) algo;
            currentSeekCost = gs.runAlgorithmLog();
            System.out.println("row.group.size=" + gs.getNumRowGroups());
            System.out.println("init.cost=" + gs.getRowGroupSize());
            System.out.println("scoa.cost=" + gs.getCurrentOverhead());
            System.out.println("init.seek.cost=" + gs.getInitSeekCost());
            System.out.println("scoa.seek.cost=" + gs.getScoaSeekCost());
        } else {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "algorithm class not fount", new Exception("algorithm error"));
        }

        return currentSeekCost;
    }

    public Object[] getLog() {
        if (algo instanceof FastScoaGSLog) {
            FastScoaGSLog gs = (FastScoaGSLog) algo;
            gs.buildLog();
            return gs.getIndices();
        } else {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "algorithm class not fount", new Exception("algorithm error"));
        }

        return null;
    }

    public void initScoaRG() {
        if (algo instanceof FastScoaGSLog) {
            FastScoaGSLog gs = (FastScoaGSLog) algo;
            gs.initScoaRG();
        } else {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "algorithm class not fount", new Exception("algorithm error"));
        }

    }

    public double run(int cx, int cy) {
        double neighbourSeekCost = -1;
        if (algo instanceof FastScoaGSLog) {
            FastScoaGSLog gs = (FastScoaGSLog) algo;
            neighbourSeekCost = gs.getRandSeekCost(cx, cy);
        } else {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "algorithm class not fount", new Exception("algorithm error"));
        }

        return neighbourSeekCost;
    }

    public double run(String cx, String cy) {
        return run(Integer.parseInt(cx), Integer.parseInt(cy));
    }
}
