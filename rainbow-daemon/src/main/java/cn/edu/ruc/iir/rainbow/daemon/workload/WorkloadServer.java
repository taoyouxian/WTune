package cn.edu.ruc.iir.rainbow.daemon.workload;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.HttpUtils;
import cn.edu.ruc.iir.rainbow.common.LogFactory;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.daemon.Server;
import cn.edu.ruc.iir.rainbow.parser.sql.parser.ParsingOptions;
import cn.edu.ruc.iir.rainbow.parser.sql.parser.SqlParser;
import cn.edu.ruc.iir.rainbow.parser.sql.tree.*;
import cn.edu.ruc.iir.rainbow.workload.cache.APCFactory;
import cn.edu.ruc.iir.rainbow.workload.cache.AccessPattern;
import cn.edu.ruc.iir.rainbow.workload.cache.AccessPatternCache;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.logging.Log;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class WorkloadServer implements Server
{
    private Log log = LogFactory.Instance().getLog();
    private volatile boolean shutdown = true;
    private Set<String> processedQueryIds = new HashSet<>();
    private String schemaName;
    private String tableName;
    private AccessPatternCache apc;
    private WorkloadQueue workloadQueue;

    /**
     * The tableName must be full table name like databaseName.tableName
     * @param schemaName
     * @param tableName
     * @param lifeTime
     * @param threshold
     */
    public WorkloadServer (String schemaName, String tableName,
                           long lifeTime, double threshold,
                           WorkloadQueue workloadQueue)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.workloadQueue = workloadQueue;
        this.apc = new AccessPatternCache(lifeTime, threshold);
        APCFactory.Instance().put(tableName, apc);
    }

    @Override
    public boolean isRunning()
    {
        if (this.shutdown == true)
        {
            return false;
        }
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
        while (shutdown == false)
        {
            this.log.info("workload server [" + this.schemaName + "." + this.tableName + "] is running...");
            try
            {
                SqlParser parser = new SqlParser();
                String jsonStr = HttpUtils.Instance().getPageContent(
                        ConfigFactory.Instance().getProperty("presto.query.url"),
                        HttpUtils.AcceptType.JSON
                        );
                JSONArray jsonArray = JSON.parseArray(jsonStr);

                for (int i = 0; i < jsonArray.size(); i++)
                {
                    JSONObject jsonObject = (JSONObject) jsonArray.get(i);
                    if (jsonObject.size() == 8)
                    {
                        // this is a valid user statement.
                        String queryId = jsonObject.getString("queryId");
                        String schema = jsonObject.getJSONObject("session").getString("schema");
                        if (schema == null)
                        {
                            continue;
                        }
                        if (this.processedQueryIds.contains(queryId) == false)
                        {
                            // this is a new statement.
                            String sql = jsonObject.getString("query");
                            long startTime = jsonObject.getJSONObject("session").getLong("startTime");
                            Statement statement = parser.createStatement(sql,
                                    new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE));
                            if (statement instanceof Query)
                            {
                                // this is a valid query.
                                Query query = (Query) statement;

                                QuerySpecification queryBody = (QuerySpecification) query.getQueryBody();
                                // get columns
                                //TODO: currently we only consider the columns in select statement.
                                List<SelectItem> selectItemList = queryBody.getSelect().getSelectItems();

                                // tableName
                                Table table = (Table) queryBody.getFrom().get();

                                String queryTableName = this.tableName;
                                if (table.getName().toString().contains("."))
                                {
                                    queryTableName = this.schemaName + "." + this.tableName;
                                }

                                if (queryTableName.equalsIgnoreCase(table.getName().toString()))
                                {
                                    this.log.info("workload server [" + this.tableName + "] is caching query: " + sql);

                                    // this is the query we care about in this pipeline (specified by tableName);
                                    AccessPattern pattern = new AccessPattern(queryId, 1.0);
                                    for (SelectItem item : selectItemList)
                                    {
                                        //TODO: currently we do not support functions and start(*) in the select statement.
                                        pattern.addColumn(item.toString());
                                    }
                                    if (this.apc.cache(pattern, startTime))
                                    {
                                        Set<AccessPattern> workload = this.apc.getAccessPatterns();
                                        this.workloadQueue.push(new ArrayList<>(workload));
                                        this.log.info("trigger layout optimization...");
                                    }

                                }
                            }
                            this.processedQueryIds.add(queryId);
                        }
                    }
                }
                // check the queries from presto query url every 5 seconds.
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "error while fetching workload from presto.", e);
            }
        }
        this.shutdown = true;
    }

}
