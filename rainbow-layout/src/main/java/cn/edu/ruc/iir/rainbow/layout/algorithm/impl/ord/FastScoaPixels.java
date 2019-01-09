package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.LogFactory;
import cn.edu.ruc.iir.rainbow.common.exception.ConfigrationException;
import cn.edu.ruc.iir.rainbow.layout.cost.PowerSeekCost;
import cn.edu.ruc.iir.rainbow.layout.cost.RealSeqReadCost;
import cn.edu.ruc.iir.rainbow.layout.cost.SeqReadCost;
import cn.edu.ruc.iir.rainbow.layout.domian.*;

import java.util.*;

/**
 * column ordering and query-wise split size optimization for Pixels
 */
public class FastScoaPixels extends FastScoa
{
    // this is the sequential read cost function
    private SeqReadCost seqReadCostFunction = null;
    private double lambdaCost = 0.0;
    private int numRowGroupPerBlock = 0;
    private boolean isSetup = false;
    // this is the seek cost of the layout in which row groups are store one by one in the block.
    private double originSeekCost = 0.0;
    // this is the seek cost after SA based ordering (but before cache optimization).
    private double orderedSeekCost = 0.0;
    // this is the total cost (seek+seqRead) when cache is applied on the origin compact columnlets
    // (columnlets of the same column are stored sequentially).
    private double startCachedCost = 0.0;
    // this is the total cached cost of the reordered columnlets.
    private double orderedCachedCost = 0.0;
    private double cacheBudgetRatio = 0.2;
    private double cacheSpaceRatio = 0.3;
    // HDFS block size
    private double blockSize = 0.0;
    // map from query id to the query's split size (number of row groups in a split).
    private Map<Integer, Integer> querySplitSizeMap = new HashMap<>();

    public FastScoaPixels ()
    {
        // read the number of row groups inside a block from configuration
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

        // read the cache budget ratio from configuration
        String strCacheBudgetRatio = ConfigFactory.Instance().getProperty("pixels.cache.compute.budget.ratio");
        try
        {
            this.cacheBudgetRatio = Double.parseDouble(strCacheBudgetRatio);
        }
        catch (Exception e)
        {
            LogFactory.Instance().getLog().error("FastScoaPixels configuration error",
                    new ConfigrationException("pixels.cache.compute.budget.ratio is not a valid number."));
        }

        // read the cache space ratio from configuration
        String strCacheSpaceRatio = ConfigFactory.Instance().getProperty("pixels.cache.space.ratio");
        try
        {
            this.cacheSpaceRatio = Double.parseDouble(strCacheSpaceRatio);
        }
        catch (Exception e)
        {
            LogFactory.Instance().getLog().error("FastScoaPixels configuration error",
                    new ConfigrationException("pixels.cache.space.ratio is not a valid number."));
        }

        // read the hostname and port of prometheus
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

        // TODO: build cost model from prometheus
        // build pixels cost model from prometheus
        //try
        //{
            //PixelsCostModel costModel = PixelsCostModelBuilder.build(promHost, promPort);
            this.setSeekCostFunction(new PowerSeekCost());
            this.setSeqReadCostFunction(new RealSeqReadCost(0.00001));
            this.setLambdaCost(10);
        //} catch (CostFunctionException e)
        //{
        //    LogFactory.Instance().getLog().error("FastScoaPixels build prometheus cost error.", e);
        //}
    }

    /**
     * <p>This function will rebuild the schema and workload for columnlet reordering inside large HDFS block.
     * In pure column ordering, we only consider the column order inside a row group. But in a block with a number
     * of row groups, we want to reorder the columnlets among all the row groups within the same block. </p>
     *
     * <p>To do that by reusing SCOA algorithm, we have to rebuild a pseudo workload and a pseudo schema.
     * We consider the columnlet access pattern in a block. In the pseudo workload, we firstly determine the split
     * size for each query in the original workload, then if there are multiple splites in a block, we build a
     * querylet (pseudo query) on each split for the original query. In pseudo schema, columnlets accessed by the
     * same set of querylets are grouped into an atomic columnlet group (ACG). Columnlets in an ACG will be moved
     * together during the columnlet reordering procedure.</p>
     */
    @SuppressWarnings("Duplicates")
    @Override
    public void setup ()
    {
        /**
         * before running setup, the initial column order and workload are given.
         */

        /**
         * we have to choose the best split size for each query, and rebuild the work
         * first, we have to read some necessary configurations.
         */
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

        double rowGroupSize = 0;
        for (Column column : this.getSchema())
        {
            rowGroupSize += column.getSize();
        }

        // get the HDFS block size.
        this.blockSize = rowGroupSize * this.numRowGroupPerBlock;

        /**
         * second, we try to calculate split size and rebuild the workload in this big loop.
         * */
        List<Query> rebuiltWorklod = new ArrayList<>();
        for (Query query : this.getWorkload())
        {
            double readSize = 0;
            for (Column column : this.getSchema())
            {
                if (query.hasColumnId(column.getId()))
                {
                    readSize += column.getSize();
                }
            }
            //double seekCost = this.getQuerySeekCost(this.getSchema(), query);
            double seqReadCost = this.seqReadCostFunction.calculate(readSize);
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
             * max split size is the max suitable split size for this query,
             * it is calculated by the limitation of memory and degree of parallelism (mapSlots).
             */
            int maxSplitSize = floor2n((int)(memSizeBytes / memAmp / mapSlots / readSize));
            if (maxSplitSize > numBlockPerNode*numRowGroupPerBlock/mapSlots/mapWaves)
            {
                maxSplitSize = numBlockPerNode*numRowGroupPerBlock/mapSlots/mapWaves;
            }
            // TODO: tmp code, should be deleted later
            // maxSplitSize = numRowGroupPerBlock;

            int splitSize = 1;

            /**
             * by this loop, we ensure the proportion of lambda cost is lower than the given threshold.
             */
            while (lambdaCost/splitSize/(seqReadCost) > lambdaThreshold)
            {
                if ((splitSize << 1) <= maxSplitSize)
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

            System.out.println(maxSplitSize + ", " + splitSize + ", " + (splitSize*readSize/1024/1024));
            // now, we set the split size for this query.
            this.querySplitSizeMap.put(query.getId(), splitSize);


            /**
             * In the rebuilt querylet, columnIds are the ids of columnlets, not the atomic columnlet group id.
             */
            // rebuild the workload
            if (splitSize < numRowGroupPerBlock)
            {
                // if there are multiple splits inside a block.

                int numSplits = numRowGroupPerBlock/splitSize;
                for (int splitId = 0; splitId < numSplits; ++splitId)
                {
                    // for each split, we fork (rebuild) a querylet (pseudo query) from the original query,
                    // the querylet accesses the columnlet in this split.
                    int rowgroupIdBase = splitId*splitSize;
                    Query rebuiltQuery = new Querylet(query.getId(), query.getSid(), query.getWeight());
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
                // if the split size >= block size, e.i. the whole block is accessed by a task.

                // we fork (rebuild) a querylet from the original query,
                // the querylet accesses all columnlets in the block.
                Query rebuiltQuery = new Querylet(query.getId(), query.getSid(), query.getWeight());
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


        // we assign the new query id for rebuilt (forked) querylets.
        // but the originId field in Querylet keeps the original query id.
        for (int i = 0; i < rebuiltWorklod.size(); ++i)
        {
            rebuiltWorklod.get(i).setId(i);
        }

        /**
         * third, rebuild schema.
         */
        // columnlets is the array list of rebuilt (forked) columnlets.
        List<Columnlet> columnlets = new ArrayList<>();
        // this is the columnlet ID to columnlet map.
        Map<Integer, Columnlet> idToColumnletMap = new HashMap<>();
        // by sequentially duplicate the columnlet, we can generally start from a very good point.
        for (Column column : this.getSchema())
        {
            for (int rowGroupId = 0; rowGroupId < numRowGroupPerBlock; ++rowGroupId)
            {
                // fork a columnlet from the original column, but query ids of the columnlet is currently empty.
                Columnlet columnlet = new Columnlet(rowGroupId, numColumns, column);
                columnlets.add(columnlet);
                idToColumnletMap.put(columnlet.getId(), columnlet);
            }
        }


        /**
         * we want to know something interesting,
         * such the origin seek cost (without sequentially columnlet duplication).
         */
        // init the originSeekCost
        List<Column> tmpColumnOrder = new ArrayList<>();
        for (int rowGroupId = 0; rowGroupId < numRowGroupPerBlock; ++rowGroupId)
        {
            for (Column column : this.getSchema())
            {
                tmpColumnOrder.add(new Columnlet(rowGroupId, numColumns, column));
            }
        }
        this.originSeekCost = this.innerGetWorkloadSeekCost(tmpColumnOrder, rebuiltWorklod);


        // set the query ids for each columnlet.
        for (Query query : rebuiltWorklod)
        {
            for (int id : query.getColumnIds())
            {
                idToColumnletMap.get(id).addQueryId(query.getId());
            }
        }

        /**
         * now, the forked columnlets have been built.
         * but we do not want this, we want an atomic grouped schema.
         */
        // build the atomic grouped schema.
        List<Column> rebuiltSchema = new ArrayList<>();
        AtomicColumnletGroup first = null;
        int acgId = 0;
        for (Columnlet columnlet : columnlets)
        {
            if (first == null || first.has(columnlet) == false)
            {
                first = new AtomicColumnletGroup(acgId++, columnlet);
                rebuiltSchema.add(first);
            }
            else
            {
                first.addColumnlet(columnlet);
            }
        }

        for (Column column : rebuiltSchema)
        {
            AtomicColumnletGroup acg = (AtomicColumnletGroup) column;
            for (int qid : acg.getQueryIds())
            {
                // for each query accesses this acg, remove the origin columnlet ids belong to this acg from the query's
                // accessed column ids.
                // note that we assigned the sequential qid to the queries in rebuiltWorkload.
                Query query = rebuiltWorklod.get(qid);
                for (Columnlet columnlet : acg.getColumnlets())
                {
                    query.getColumnIds().remove(columnlet.getId());
                }
            }
        }

        for (Column column : rebuiltSchema)
        {
            AtomicColumnletGroup acg = (AtomicColumnletGroup) column;
            for (int qid : acg.getQueryIds())
            {
                // for each query accesses this acg, add the column id of this acg to the columnIds of the query.
                // note that we assigned the sequential qid to the queries in rebuiltWorkload.
                Query query = rebuiltWorklod.get(qid);
                // id of acg is re-assigned
                query.addColumnId(acg.getId());
            }
        }

        /**
         * now we have rebuilt the workload and schema.
         */

        // update schema and workload
        this.setSchema(rebuiltSchema);
        this.setWorkload(rebuiltWorklod);
        // setup supper, this will prepare the structures
        super.setup();

        // update parameters
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

        // setup finished
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
        for (int n = 0; n < 31 && (1<<n) <= i; ++n)
        {
            res = 1<<n;
        }
        return res;
    }

    @SuppressWarnings("Duplicates")
    @Override
    public void runAlgorithm()
    {

        /**
         * we first do acg ordering.
         */

        //computation budget (seconds) for acg ordering.
        long orderingBudget = (long) (this.getComputationBudget() * (1-this.cacheBudgetRatio));

        long startSeconds = System.currentTimeMillis() / 1000;
        this.currentEnergy = this.getCurrentWorkloadSeekCost();

        for (long currentSeconds = System.currentTimeMillis() / 1000;
             (currentSeconds - startSeconds) < orderingBudget;
             currentSeconds = System.currentTimeMillis() / 1000, ++this.iterations)
        {
            //generate two random indices
            int i = rand.nextInt(this.getColumnOrder().size());
            int j = i;
            while (j == i)
                j = rand.nextInt(this.getColumnOrder().size());
            rand.setSeed(System.nanoTime());

            //calculate new cost
            double neighbourEnergy = this.getNeighbourSeekCost(i, j);

            //try to accept it
            double temperature = this.getTemperature();
            if (this.probability(currentEnergy, neighbourEnergy, temperature) > Math.random())
            {
                currentEnergy = neighbourEnergy;
                updateColumnOrder(i, j);
            }
        }

        // records the ordered seek cost.
        this.orderedSeekCost = this.getCurrentWorkloadSeekCost();


        /**
         * then, we do cache optimization
         */

        // the computation budget (seconds) for cache optimization
        long cacheComputeBudget = this.getComputationBudget() - orderingBudget;

        startSeconds = System.currentTimeMillis() / 1000;
        this.currentEnergy = this.getCurrentCachedCost();
        // records the start cached cost.
        this.startCachedCost = this.currentEnergy;
        // we do not need the annealing.
        this.temperature = 0;

        if (this.cacheSpaceRatio > 0)
        {
            for (long currentSeconds = System.currentTimeMillis() / 1000;
                 (currentSeconds - startSeconds) < cacheComputeBudget;
                 currentSeconds = System.currentTimeMillis() / 1000, ++this.iterations/*this.iterations is cumulative*/)
            {
                // we get the cached neighbour by randomly swap two atomic column group,
                // and get the cached cost of real columnlet order. This is not a bug. for that
                // we want to ensure columnlets in the same group are moved together.
                List<Column> neighbour = this.getCachedNeighbour();
                double neighbourEnergy = this.getCachedCost(this.getRealColumnletOrder(neighbour));

                this.accept(neighbour, neighbourEnergy);
            }
        }

        // records the ordered cached cost.
        this.orderedCachedCost = this.getCurrentCachedCost();

    }

    /**
     * given the cache space ratio and the column order, calculate the cache border.
     * column in the column order with index < cache border will be cached.
     * if cache border is 0, it means there is no column can be cached.
     * this method is not thread safe.
     * @return
     */
    public int getCacheBorder (List<Column> columnOrder)
    {
        double cacheSpace = this.cacheSpaceRatio * this.blockSize;
        int cacheBorder = 0;
        double cachedSize = 0;
        for (Column column : columnOrder)
        {
            cachedSize += column.getSize();
            if (cachedSize >= cacheSpace)
            {
                break;
            }
            cacheBorder++;
        }

        return cacheBorder;
    }

    public int getNumRowGroupPerBlock ()
    {
        return this.numRowGroupPerBlock;
    }

    /**
     * this method is not thread safe.
     * It must be called immediately after getting cacheBorder (before cacheBorder may change).
     * @return
     */
    private List<Column> getCachedNeighbour ()
    {
        List<Column> neighbour = new ArrayList<>(this.getColumnOrder());
        int cacheBorder = this.getCacheBorder(this.getColumnOrder());

        if (cacheBorder == 0)
        {
            return neighbour;
        }

        int i = rand.nextInt(cacheBorder);
        int j = rand.nextInt(neighbour.size()-cacheBorder) + cacheBorder;

        Column c = neighbour.get(i);
        neighbour.set(i, neighbour.get(j));
        neighbour.set(j, c);
        rand.setSeed(System.nanoTime());

        return neighbour;
    }

    public int getQuerySplitSize(int queryId)
    {
        return this.querySplitSizeMap.get(queryId);
    }


    /**
     * get the cached cost (sequential read cost + seek cost) of the given columnlet order on the
     * current workload.
     * @param columnOrder the real columnlet order, not the atomic column group order
     * @return
     */
    public double getColumnOrderCachedCost (List<Column> columnOrder)
    {
        return this.getCachedCost(columnOrder);
    }


    /**
     * get the cached cost (sequential read cost + seek cost) of the given columnlet order on the
     * current workload.
     * @param columnOrder the real columnlet order, not the atomic column group order
     * @return
     */
    @SuppressWarnings("Duplicates")
    private double getCachedCost (List<Column> columnOrder)
    {
        int cacheBorder = this.getCacheBorder(columnOrder);

        Comparator<Integer> naturalOrder = Comparator.naturalOrder();

        List<Column> uncachedColumnOrder = columnOrder.subList(cacheBorder, columnOrder.size());

        Set<Integer> cacheColumnIds = new HashSet<>();

        for (int i = 0; i < cacheBorder; ++i)
        {
            cacheColumnIds.add(columnOrder.get(i).getId());
        }

        //make a prefix summation array
        List<Double> sumSize = new ArrayList<Double>();

        for (int i = 0; i < uncachedColumnOrder.size(); i++)
        {
            if (i == 0)
            {
                sumSize.add(0.0);
            }
            else
            {
                sumSize.add(sumSize.get(i - 1) + uncachedColumnOrder.get(i - 1).getSize());
            }
        }

        //mark a map from column id to neighbor index
        Map<Integer, Integer> uncachedIdMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < uncachedColumnOrder.size(); i++)
        {
            uncachedIdMap.put(uncachedColumnOrder.get(i).getId(), i);
        }

        /**
        Map<Integer, Integer> allIdMap = new HashMap<>();
        for (int i = 0; i < columnOrder.size(); ++i)
        {
            allIdMap.put(columnOrder.get(i).getId(), i);
        }
        */

        //calculate cost
        double workloadCost = 0;
        for (Query query : super.getWorkload())
        {
            // curColumns is the index of each column in the neighbour ordering.
            List<Integer> curColumns = new ArrayList<>();
            double querySeqReadCost = 0.0;
            for (int cid : query.getColumnIds())
            {
                if (cacheColumnIds.contains(cid) == false)
                {
                    curColumns.add(uncachedIdMap.get(cid));
                    Column column = columnOrder.get(uncachedIdMap.get(cid));
                    querySeqReadCost += this.getSeqReadCostFunction().calculate(column.getSize());
                }
            }
            curColumns.sort(naturalOrder);

            double querySeekCost = 0;
            double lastOffset = 0;
            for (int i = 0; i < curColumns.size(); i++)
            {
                // id is the index of the column in neighbour
                int id = curColumns.get(i);
                if (lastOffset != 0)
                {
                    querySeekCost += this.getSeekCostFunction().calculate((long)lastOffset, sumSize.get(id) - lastOffset);
                }
                lastOffset = sumSize.get(id) + uncachedColumnOrder.get(id).getSize();
            }
            workloadCost += query.getWeight() * (querySeekCost + querySeqReadCost);
        }
        return workloadCost;
    }

    public double getCurrentCachedCost ()
    {
        //return this.getCachedCost(this.getColumnOrder());
        return this.getCachedCost(this.getRealColumnletOrder());
    }

    public double getOriginSeekCost()
    {
        return originSeekCost;
    }

    public double getOrderedSeekCost()
    {
        return orderedSeekCost;
    }

    /**
     * get the cached cost of the columnlet order and workload before columnlet ordering.
     * @return
     */
    public double getStartCachedCost()
    {
        return startCachedCost;
    }

    /**
     * get the cached cost of the columnlet order and workload after columnlet ordering.
     * @return
     */
    public double getOrderedCachedCost()
    {
        return orderedCachedCost;
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
     * get the real column order from the given acg order.
     * real column order is not the rebuilt column order.
     * origin column ids can be got by getColumnId method of the columnlets in read column order.
     * @param atomicColumnGroupOrder
     * @return
     */
    public List<Column> getRealColumnletOrder(List<Column> atomicColumnGroupOrder)
    {
        List<Column> columnOrder = new ArrayList<>();
        for (Column column : atomicColumnGroupOrder)
        {
            AtomicColumnletGroup acg = (AtomicColumnletGroup) column;
            for (Columnlet columnlet : acg.getColumnlets())
            {
                columnOrder.add(columnlet);
            }
        }
        return columnOrder;
    }

    /**
     * get the real column order of the current column order (acg order).
     * real column order is not the rebuilt column order.
     * origin column ids can be got by getColumnId method of the columnlets in read column order.
     * @return
     */
    public List<Column> getRealColumnletOrder()
    {
        return this.getRealColumnletOrder(this.getColumnOrder());
    }


    /**
     * given the columnorder, get the total seek cost of all querylets in the workload.
     * @param columnOrder the real columnlet order, not the atomic column group order.
     * @param workload the querylets.
     * @return
     */
    private double innerGetWorkloadSeekCost(List<Column> columnOrder, List<Query> workload)
    {
        double workloadSeekCost = 0;
        // originIdToQueryletsMap is redundant.
        Map<Integer, List<Query>> originIdToQueryletsMap = new HashMap<>();
        for (Query query : workload)
        {
            Querylet querylet = (Querylet) query;
            if (originIdToQueryletsMap.containsKey(querylet.getOriginId()))
            {
                originIdToQueryletsMap.get(querylet.getOriginId()).add(query);
            }
            else
            {
                List<Query> queries = new ArrayList<>();
                queries.add(query);
                originIdToQueryletsMap.put(querylet.getOriginId(), queries);
            }
        }

        for (Map.Entry<Integer, List<Query>> entry : originIdToQueryletsMap.entrySet())
        {
            double seekCost = 0;
            for (Query query : entry.getValue())
            {
                seekCost += query.getWeight() * getQuerySeekCost(columnOrder, query);
            }
            // note: it is currently not reasonable to use average seek cost.
            workloadSeekCost += seekCost;// / entry.getValue().size();
        }

        return workloadSeekCost;
    }

    /**
     * given the column order, get the total sequential read cost of all querylets in the workload.
     * @param columnOrder the real columnlet order, not the atomic column group order.
     * @param workload the querylets.
     * @return
     */
    private double innerGetWorkloadSeqReadCost(List<Column> columnOrder, List<Query> workload)
    {
        double readCost = 0;
        for (Query query : workload)
        {
            readCost += query.getWeight() * this.getQuerySeqReadCost(columnOrder, query);
        }
        return readCost;
    }

    /**
     * get the sequential read cost of a query.
     * @param columnOrder the real columnlet order, not the atomic column group order
     * @param query the querylet.
     * @return
     */
    private double getQuerySeqReadCost (List<Column> columnOrder, Query query)
    {
        double seqReadCost = 0;
        int accessedColumnNum = 0;
        for (Column column : columnOrder)
        {
            // it is right to use column.getId(), e.i. the columnlet id.
            if (query.getColumnIds().contains(column.getId()))
            {
                // column i has been accessed by the query
                seqReadCost += this.seqReadCostFunction.calculate(column.getSize());
                accessedColumnNum++;
                if (accessedColumnNum >= query.getColumnIds().size())
                {
                    // the query has accessed all the necessary columns
                    break;
                }
            }
        }
        return seqReadCost;
    }

    /**
     * get the seek cost of the whole workload (on the current column order).
     * @return
     */
    @Override
    public double getCurrentWorkloadSeekCost()
    {
        if (this.isSetup)
        {
            return innerGetWorkloadSeekCost(this.getRealColumnletOrder(), this.getWorkload());
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
            return innerGetWorkloadSeekCost(this.getRealColumnletOrder(this.getSchema()), this.getWorkload());
        }
        else
        {
            return this.numRowGroupPerBlock * super.getSchemaSeekCost();
        }
    }

    /**
     * in this function, if there are 16 row groups in a block, there will be 16x queries and 16x columns,
     * and the seek cost and sequential read cost of these queries on these columns are calculated.
     * @return
     */
    public double getCurrentWorkloadCost()
    {
        return this.getCurrentWorkloadSeekCost() +
                this.innerGetWorkloadSeqReadCost(this.getRealColumnletOrder(), this.getWorkload());
    }

    public double getSchemaCost()
    {
        return this.getSchemaSeekCost() +
                this.innerGetWorkloadSeqReadCost(this.getRealColumnletOrder(this.getSchema()), this.getWorkload());
    }

    /**
     * get the seek cost of a query (on the given column order).
     * this is a general function, sub classes can override it.
     * @param columnOrder the real columnlet order, not the atomic column group order
     * @param query the querylet
     * @return
     */
    @SuppressWarnings("Duplicates")
    @Override
    protected double getQuerySeekCost(List<Column> columnOrder, Query query)
    {
        double querySeekCost = 0, seekDistance = 0;
        int accessedColumnNum = 0;
        // we do not consider the initial seek cost,
        // so that we ignore seek cost from beginning to the first accessed column.
        // NOTICE: long ago, we did not consider the initial seek cost.
        // But we changed our mind, we want to consider the initial seek cost
        //boolean finishFirstRead = false;
        double lastOffset = 0;
        for (int i = 0; i < columnOrder.size(); ++i)
        {
            if (query.getColumnIds().contains(columnOrder.get(i).getId()))
            {
                // column i has been accessed by the query
                //if (finishFirstRead == false)
                {
                //    finishFirstRead = true;
                }
                //else
                {
                    querySeekCost += this.getSeekCostFunction().calculate((long)lastOffset, seekDistance);
                }
                // TODO: check if lastOffset is set correctly.
                lastOffset += seekDistance;
                seekDistance = 0;
                accessedColumnNum++;

                if (accessedColumnNum >= query.getColumnIds().size())
                {
                    // the query has accessed all the necessary columns
                    break;
                }
            } else
            {
                //if (finishFirstRead == true)
                {
                    // column i has been skipped (seek over) by the query
                    seekDistance += columnOrder.get(i).getSize();
                }
            }
        }
        return querySeekCost;
    }
}
