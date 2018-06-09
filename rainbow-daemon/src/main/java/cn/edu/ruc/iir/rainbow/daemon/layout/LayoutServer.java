package cn.edu.ruc.iir.rainbow.daemon.layout;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.LogFactory;
import cn.edu.ruc.iir.rainbow.common.exception.*;
import cn.edu.ruc.iir.rainbow.daemon.Server;
import cn.edu.ruc.iir.rainbow.daemon.workload.WorkloadQueue;
import cn.edu.ruc.iir.rainbow.layout.algorithm.Algorithm;
import cn.edu.ruc.iir.rainbow.layout.algorithm.AlgorithmFactory;
import cn.edu.ruc.iir.rainbow.layout.algorithm.ExecutorContainer;
import cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord.FastScoaPixels;
import cn.edu.ruc.iir.rainbow.layout.builder.ColumnOrderBuilder;
import cn.edu.ruc.iir.rainbow.layout.builder.WorkloadBuilder;
import cn.edu.ruc.iir.rainbow.layout.builder.domain.CompactLayoutObj;
import cn.edu.ruc.iir.rainbow.layout.builder.domain.InitOrderObj;
import cn.edu.ruc.iir.rainbow.layout.builder.domain.SplitPatternObj;
import cn.edu.ruc.iir.rainbow.layout.builder.domain.SplitStrategyObj;
import cn.edu.ruc.iir.rainbow.layout.cost.PowerSeekCost;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Columnlet;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.rainbow.layout.model.ColumnModel;
import cn.edu.ruc.iir.rainbow.layout.model.LayoutModel;
import cn.edu.ruc.iir.rainbow.layout.model.SchemaModel;
import cn.edu.ruc.iir.rainbow.layout.model.TableModel;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Layout;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Schema;
import cn.edu.ruc.iir.rainbow.layout.model.domain.Table;
import cn.edu.ruc.iir.rainbow.workload.cache.AccessPattern;
import com.alibaba.fastjson.JSON;
import org.apache.commons.logging.Log;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class LayoutServer implements Server
{
    private Log log = LogFactory.Instance().getLog();
    private volatile boolean shutdown = true;
    private String schemaName;
    private String tableName;
    private WorkloadQueue workloadQueue;

    public LayoutServer (String schemaName, String tableName, WorkloadQueue workloadQueue)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.workloadQueue = workloadQueue;
    }

    @Override
    public boolean isRunning()
    {
        return Thread.currentThread().isAlive();
    }

    @Override
    public void shutdown()
    {
        this.shutdown = true;
    }

    @Override
    public void run()
    {
        this.shutdown = false;
        while (this.shutdown == false)
        {
            this.log.info("layout server [" + this.schemaName + "." + this.tableName + "] is running...");
            try
            {
                Set<AccessPattern> accessPatterns = workloadQueue.pop();

                SchemaModel schemaModel = new SchemaModel();
                TableModel tableModel = new TableModel();
                LayoutModel layoutModel = new LayoutModel();
                ColumnModel columnModel = new ColumnModel();
                Schema schema = schemaModel.getByName(this.schemaName);
                Table table = tableModel.getByNameAndSchema(this.tableName, schema);
                // there are only one writable layout in a table.
                Layout prevLayout = layoutModel.getWritableByTable(table);

                List<Column> initColumnOrder = ColumnOrderBuilder.wrappedColumns(columnModel.getByTable(table));
                List<Query> workload = wrappedWorkload(accessPatterns, initColumnOrder);

                List<Column> prevColumnOrder = new ArrayList<>();
                List<Column> prevCompactLayout = new ArrayList<>();
                String initOrderJson = prevLayout.getInitOrder();
                String compactLayoutJson = prevLayout.getCompact();
                List<String> prevColumnNameOrder = JSON.parseObject(initOrderJson, InitOrderObj.class).getColumnOrder();
                Map<String, Column> nameToColumnMap = new HashMap<>();
                for (Column column : initColumnOrder)
                {
                    nameToColumnMap.put(column.getName(), column);
                }
                for (String columnName : prevColumnNameOrder)
                {
                    prevColumnOrder.add(nameToColumnMap.get(columnName));
                }

                CompactLayoutObj compactLayoutObj = JSON.parseObject(compactLayoutJson, CompactLayoutObj.class);
                int numRowGroups = compactLayoutObj.getRowGroupNumber();
                int numColumns = compactLayoutObj.getColumnNumber();
                List<String> columnletIdOrder = compactLayoutObj.getColumnletOrder();
                for (String columnletId : columnletIdOrder)
                {
                    String[] splits = columnletId.split(":");
                    int rowGroupId = Integer.parseInt(splits[0]);
                    int columnIndex = Integer.parseInt(splits[1]);
                    Columnlet columnlet = new Columnlet(rowGroupId, numColumns, prevColumnOrder.get(columnIndex));
                    prevCompactLayout.add(columnlet);
                }

                try
                {
                    List<Column> currentColumnOrder;
                    List<Column> currentCompactLayout;

                    Algorithm scoa = AlgorithmFactory.Instance().getAlgorithm("scoa", 200, new ArrayList<>(prevColumnOrder), workload, new PowerSeekCost());
                    ExecutorContainer container = new ExecutorContainer(scoa, 1);
                    container.waitForCompletion(1, percentage -> {
                        System.out.println(percentage);
                    });

                    double prevOrderedSeekCost = scoa.getWorkloadSeekCost(workload, prevColumnOrder);
                    double currentOrderedSeekCost = scoa.getCurrentWorkloadSeekCost();

                    if (currentOrderedSeekCost < prevOrderedSeekCost)
                    {
                        currentColumnOrder = scoa.getColumnOrder();
                    }
                    else
                    {
                        currentColumnOrder = prevColumnOrder;
                    }

                    FastScoaPixels scoaPixels = (FastScoaPixels) AlgorithmFactory.Instance().getAlgorithm("scoa.pixels", 300, new ArrayList<>(currentColumnOrder), workload);
                    container = new ExecutorContainer(scoaPixels, 1);
                    container.waitForCompletion(1, percentage -> {
                        System.out.println(percentage);
                    });

                    if (prevCompactLayout.isEmpty() == false)
                    {
                        double prevCachedCost = scoaPixels.getColumnOrderCachedCost(prevCompactLayout);
                        double currentCachedCost = scoaPixels.getOrderedCachedCost();
                        if (currentCachedCost < prevCachedCost)
                        {
                            currentCompactLayout = scoaPixels.getRealColumnletOrder();
                        }
                        else
                        {
                            if (currentColumnOrder == prevColumnOrder)
                            {
                                // no better column order and compact layout found.
                                return;
                            }
                            currentCompactLayout = prevCompactLayout;
                        }
                    }
                    else
                    {
                        currentCompactLayout = scoaPixels.getRealColumnletOrder();
                    }

                    prevLayout.setWritable(false);

                    String warehousePath = ConfigFactory.Instance().getProperty("pixels.warehouse.path");
                    if (! (warehousePath.endsWith("/") || warehousePath.endsWith("\\")))
                    {
                        warehousePath += "/";
                    }

                    String currentPath = warehousePath + schemaName + "/" + tableName + "/" + (prevLayout.getVersion()+1);

                    Map<Integer, Integer> columnIdToIndexMap = new HashMap<>();
                    for (int i = 0; i < currentColumnOrder.size(); ++i)
                    {
                        columnIdToIndexMap.put(currentColumnOrder.get(i).getId(), i);
                    }
                    SplitStrategyObj currSplitStrategy = new SplitStrategyObj();
                    currSplitStrategy.setNumRowGroupInBlock(scoaPixels.getNumRowGroupPerBlock());
                    for (Query query : workload)
                    {
                        SplitPatternObj splitPattern = new SplitPatternObj();
                        splitPattern.setNumRowGroupInSplit(scoaPixels.getQuerySplitSize(query.getId()));
                        for (int cid : query.getColumnIds())
                        {
                            splitPattern.addAccessedColumns(columnIdToIndexMap.get(cid));
                        }
                        currSplitStrategy.addSplitPatterns(splitPattern);
                    }


                    Layout currentLayout = new Layout();
                    currentLayout.setReadable(true);
                    currentLayout.setWritable(true);
                    currentLayout.setTable(table);
                    currentLayout.setVersion(prevLayout.getVersion()+1);
                    currentLayout.setSplit(JSON.toJSONString(currSplitStrategy));
                    currentLayout.setCompactPath(currentPath + "_compact");
                    currentLayout.setInitPath(currentPath + "_init");
                    currentLayout.setInitOrder(ColumnOrderBuilder.initOrderToJsonString(currentColumnOrder));
                    currentLayout.setCompact(ColumnOrderBuilder.compactLayoutToJsonString(numRowGroups, numColumns, currentCompactLayout));
                    currentLayout.setCreateAt(System.currentTimeMillis());
                    currentLayout.setEnabledAt(System.currentTimeMillis());
                    currentLayout.setId(-1);

                    layoutModel.save(currentLayout);
                    layoutModel.save(prevLayout);


                } catch (NotMultiThreadedException e)
                {
                    ExceptionHandler.Instance().log(ExceptionType.ERROR, "thread number is " + 1, e);
                } catch (AlgoException e)
                {
                    ExceptionHandler.Instance().log(ExceptionType.ERROR, "error while running algorithm", e);
                } catch (ClassNotFoundException e)
                {
                    ExceptionHandler.Instance().log(ExceptionType.ERROR, "algorithm class not fount", e);
                }


                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "error while fetching workload from workload queue.", e);
            } catch (ColumnNotFoundException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "error while wrapping workload.", e);
            }
        }
    }

    public static List<Query> wrappedWorkload (Set<AccessPattern> accessPatterns, List<Column> columnOrder)
            throws ColumnNotFoundException
    {
        Map<String, Column> columnMap = new HashMap<String, Column>();
        List<Query> workload = new ArrayList<Query>();

        for (Column column : columnOrder)
        {
            columnMap.put(column.getName().toLowerCase(), column);
        }

        int qid = 0;
        for (AccessPattern accessPattern : accessPatterns)
        {
            Set<String> columnNames = accessPattern.getColumns();
            Query query = new Query(qid, accessPattern.getQueryId(), 1.0);
            for (String columnName : columnNames)
            {
                Column column = columnMap.get(columnName.toLowerCase());
                if (column == null)
                {
                    throw new ColumnNotFoundException("column " + columnName + " from query " +
                            accessPattern.getQueryId() + " is not found in the schema.");
                }
                query.addColumnId(column.getId());
            }
            boolean noEquals = true;
            for (Query query1 : workload)
            {
                if (WorkloadBuilder.equalsColumnAccessSet(query.getColumnIds(), query1.getColumnIds()))
                {
                    query1.addWeight(query.getWeight());
                    noEquals = false;
                    break;
                }
            }
            if (noEquals)
            {
                qid++;
                workload.add(query);
            }
        }

        return workload;
    }
}
