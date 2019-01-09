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
                 * the columns get by columnModel are sorted by cid, and the index in this sorted column array are
                 * used as the column id in all column orders in this function.
                 */
                List<Column> initColumnOrder = ColumnOrderBuilder.wrappedColumns(columnModel.getByTable(table));
                if (prevLayout != null)
                {
                    /**
                     * if there is a existing previous layout, then use the column order in this layout as the initColumnOrder.
                     * but the column ids are not re-assigned.
                     */
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

                // generate the workload
                List<Query> workload = wrappedWorkload(accessPatterns, initColumnOrder);
                System.out.println("workload size: " + workload.size());

                try
                {
                    // the new column order to be calculated.
                    List<Column> currentColumnOrder;
                    // the new compact layout to be calculated
                    List<Column> currentCompactLayout;

                    // the version of new layout.
                    int currentVersion = 0;
                    if (prevLayout != null)
                    {
                        currentVersion = prevLayout.getVersion() + 1;
                    }

                    System.out.println("running scoa...");

                    // running scoa from initColumnOrder
                    Algorithm scoa = AlgorithmFactory.Instance().getAlgorithm("scoa", 400,
                            new ArrayList<>(initColumnOrder), new ArrayList<>(workload), new PowerSeekCost());
                    System.out.println("schema seek cost:" + scoa.getSchemaSeekCost());

                    ExecutorContainer container = new ExecutorContainer(scoa, 1);
                    container.waitForCompletion(1, percentage -> System.out.println(percentage));

                    double currentOrderedSeekCost = scoa.getCurrentWorkloadSeekCost();
                    System.out.println("ordered seek cost:" + currentOrderedSeekCost);

                    /**
                     * now we get the new column order.
                     */
                    currentColumnOrder = scoa.getColumnOrder();

                    System.out.println("running scoa pixels...");

                    // running scoaPixels from new column order.
                    FastScoaPixels scoaPixels = (FastScoaPixels) AlgorithmFactory.Instance().getAlgorithm(
                            "scoa.pixels", 600, new ArrayList<>(currentColumnOrder),
                            new ArrayList<>(workload));



                    container = new ExecutorContainer(scoaPixels, 1);
                    System.out.println("init seek cost:" + scoaPixels.getCurrentWorkloadSeekCost());
                    container.waitForCompletion(1, percentage -> System.out.println(percentage));


                    System.out.println("origin seek cost:" + scoaPixels.getOriginSeekCost());

                    System.out.println("ordered seek cost:" + scoaPixels.getOrderedSeekCost());

                    System.out.println("start cached cost:" + scoaPixels.getStartCachedCost());
                    double currentCachedCost = scoaPixels.getOrderedCachedCost();
                    System.out.println("current cached cost:" + currentCachedCost);

                    /**
                     * new we get the new compact layout.
                     * but the column ids (can be got by getColumnId, not getId())
                     * in this compact layout is the column ids in initColumnOrder,
                     * which is not what we want in the metadata.
                     */
                    currentCompactLayout = scoaPixels.getRealColumnletOrder();

                    /**
                     * <b><begin</b>
                     * build the relative compact layout, in which the column id is the index of column in currentColumnOrder.
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
                     * <b>end</b>
                     */

                    // get the store path for new ordered and compact layout.
                    String warehousePath = ConfigFactory.Instance().getProperty("pixels.warehouse.path");
                    if (! (warehousePath.endsWith("/") || warehousePath.endsWith("\\")))
                    {
                        warehousePath += "/";
                    }
                    String currentBasePath = warehousePath + schemaName + "/" + tableName + "/v_" + currentVersion;

                    /**
                     * get the split strategy. column ids in query's getColumnIds are the column ids in initColumnOrder,
                     * we have to convert such column ids into relative column ids.
                     */
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

                    /**
                     * now we get the new layout to be store into metadata.
                     */
                    Layout currentLayout = new Layout();

                    currentLayout.setId(-1);// layout with id < 0 will be saved as a new layout into metadata.
                    currentLayout.setPermission(-1);// layout with permission < 0 is not readable nor writable.
                    currentLayout.setVersion(currentVersion);
                    currentLayout.setCreateAt(System.currentTimeMillis());
                    currentLayout.setOrder(ColumnOrderBuilder.orderToJsonString(currentColumnOrder));
                    currentLayout.setOrderPath(currentBasePath + "_order");
                    currentLayout.setCompact(ColumnOrderBuilder.compactLayoutToJsonString(scoaPixels.getNumRowGroupPerBlock(),
                            initColumnOrder.size(), scoaPixels.getCacheBorder(currentCompactLayout), relativeCompactLayout));
                    currentLayout.setCompactPath(currentBasePath + "_compact");
                    currentLayout.setSplits(JSON.toJSONString(currSplitStrategy));
                    currentLayout.setTable(table);

                    /**
                     * save the new layout into metadata.
                     */
                    //layoutModel.save(currentLayout);


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

                /**
                 * have a rest.
                 */
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

    /**
     * generate the workload for layout optimization. query ids are sequentially assigned.
     * @param accessPatterns
     * @param columnOrder
     * @return
     * @throws ColumnNotFoundException
     */
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
