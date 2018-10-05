package cn.edu.ruc.iir.rainbow.daemon.layout;

import cn.edu.ruc.iir.pixels.common.metadata.domain.Order;
import cn.edu.ruc.iir.pixels.common.metadata.domain.SplitPattern;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Splits;
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
import cn.edu.ruc.iir.rainbow.layout.cost.PowerSeekCost;
import cn.edu.ruc.iir.rainbow.layout.domian.Column;
import cn.edu.ruc.iir.rainbow.layout.domian.Columnlet;
import cn.edu.ruc.iir.rainbow.layout.domian.Query;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.ColumnDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.LayoutDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.SchemaDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.TableDao;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Table;
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
                List<AccessPattern> accessPatterns = workloadQueue.pop();

                SchemaDao schemaModel = new SchemaDao();
                TableDao tableModel = new TableDao();
                LayoutDao layoutModel = new LayoutDao();
                ColumnDao columnModel = new ColumnDao();
                Schema schema = schemaModel.getByName(this.schemaName);
                Table table = tableModel.getByNameAndSchema(this.tableName, schema);
                // there are only one writable layout in a table.
                Layout prevLayout = layoutModel.getLatestByTable(table);

                /**
                 *
                 */
                List<Column> initColumnOrder = ColumnOrderBuilder.wrappedColumns(columnModel.getByTable(table));
                if (prevLayout != null)
                {
                    List<Column> prevColumnOrder = new ArrayList<>();
                    String initOrderJson = prevLayout.getOrder();
                    List<String> prevColumnNameOrder = JSON.parseObject(initOrderJson, Order.class).getColumnOrder();
                    Map<String, Column> nameToColumnMap = new HashMap<>();
                    for (Column column : initColumnOrder)
                    {
                        nameToColumnMap.put(column.getName(), column);
                    }
                    for (String columnName : prevColumnNameOrder)
                    {
                        prevColumnOrder.add(nameToColumnMap.get(columnName));
                    }
                    initColumnOrder = prevColumnOrder;
                }
                List<Query> workload = wrappedWorkload(accessPatterns, initColumnOrder);

                try
                {
                    List<Column> currentColumnOrder;
                    List<Column> currentCompactLayout;

                    int currentVersion = 0;
                    if (prevLayout != null)
                    {
                        currentVersion = prevLayout.getVersion() + 1;
                    }

                    System.out.println("running scoa...");

                    Algorithm scoa = AlgorithmFactory.Instance().getAlgorithm("scoa", 400, new ArrayList<>(initColumnOrder), workload, new PowerSeekCost());
                    ExecutorContainer container = new ExecutorContainer(scoa, 1);
                    container.waitForCompletion(1, percentage -> {
                        System.out.println(percentage);
                    });

                    double currentOrderedSeekCost = scoa.getCurrentWorkloadSeekCost();
                    System.out.println("current ordered seek cost:" + currentOrderedSeekCost);

                    currentColumnOrder = scoa.getColumnOrder();

                    System.out.println("running scoa pixels...");

                    FastScoaPixels scoaPixels = (FastScoaPixels) AlgorithmFactory.Instance().getAlgorithm("scoa.pixels", 600, new ArrayList<>(currentColumnOrder), workload);
                    container = new ExecutorContainer(scoaPixels, 1);
                    container.waitForCompletion(1, percentage -> {
                        System.out.println(percentage);
                    });
                    System.out.println("start cached cost: " + scoaPixels.getStartCachedCost());
                    double currentCachedCost = scoaPixels.getOrderedCachedCost();
                    System.out.println("current cached cost: " + currentCachedCost);
                    currentCompactLayout = scoaPixels.getRealColumnletOrder();

                    /**
                     * begin
                     * build the relative compact layout, in which the column id is the index of column in currentColumnOrder.
                     * TODO: to be tested.
                     */
                    int[] columnIdToCurrenIndex = new int[currentColumnOrder.size()];

                    for (int i = 0; i < columnIdToCurrenIndex.length; ++i)
                    {
                        columnIdToCurrenIndex[currentColumnOrder.get(i).getId()] = i;
                    }

                    List<Column> relativeCompactLayout = new ArrayList<>();
                    for (Column column : currentCompactLayout)
                    {
                        Columnlet columnlet = (Columnlet) column;
                        Columnlet relativeColumnlet = new Columnlet(columnlet.getRowGroupId(), columnIdToCurrenIndex[columnlet.getColumnId()],
                                columnlet.getNumColumns(), columnlet.getName(), columnlet.getType(), columnlet.getSize());
                        relativeCompactLayout.add(relativeColumnlet);
                    }
                    /**
                     * end
                     */

                    String warehousePath = ConfigFactory.Instance().getProperty("pixels.warehouse.path");
                    if (! (warehousePath.endsWith("/") || warehousePath.endsWith("\\")))
                    {
                        warehousePath += "/";
                    }

                    String currentBasePath = warehousePath + schemaName + "/" + tableName + "/v_" + currentVersion;

                    Splits currSplitStrategy = new Splits();
                    currSplitStrategy.setNumRowGroupInBlock(scoaPixels.getNumRowGroupPerBlock());
                    for (Query query : workload)
                    {
                        SplitPattern splitPattern = new SplitPattern();
                        splitPattern.setNumRowGroupInSplit(scoaPixels.getQuerySplitSize(query.getId()));
                        for (int cid : query.getColumnIds())
                        {
                            splitPattern.addAccessedColumns(columnIdToCurrenIndex[cid]);
                        }
                        currSplitStrategy.addSplitPatterns(splitPattern);
                    }

                    Layout currentLayout = new Layout();
                    currentLayout.setPermission(-1);
                    currentLayout.setTable(table);
                    currentLayout.setVersion(currentVersion);
                    currentLayout.setSplits(JSON.toJSONString(currSplitStrategy));
                    currentLayout.setCompactPath(currentBasePath + "_compact");
                    currentLayout.setOrderPath(currentBasePath + "_order");
                    currentLayout.setOrder(ColumnOrderBuilder.orderToJsonString(currentColumnOrder));
                    currentLayout.setCompact(ColumnOrderBuilder.compactLayoutToJsonString(scoaPixels.getNumRowGroupPerBlock(),
                            initColumnOrder.size(), scoaPixels.getCacheBorder(currentCompactLayout), relativeCompactLayout));
                    currentLayout.setCreateAt(System.currentTimeMillis());
                    currentLayout.setId(-1);

                    layoutModel.save(currentLayout);


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

    public static List<Query> wrappedWorkload (List<AccessPattern> accessPatterns, List<Column> columnOrder)
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
