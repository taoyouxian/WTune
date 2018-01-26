package cn.edu.ruc.iir.rainbow.daemon.workload;

import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.util.HttpUtil;
import cn.edu.ruc.iir.rainbow.daemon.Server;
import cn.edu.ruc.iir.rainbow.parser.sql.parser.ParsingOptions;
import cn.edu.ruc.iir.rainbow.parser.sql.parser.SqlParser;
import cn.edu.ruc.iir.rainbow.parser.sql.tree.Query;
import cn.edu.ruc.iir.rainbow.parser.sql.tree.QuerySpecification;
import cn.edu.ruc.iir.rainbow.parser.sql.tree.SelectItem;
import cn.edu.ruc.iir.rainbow.parser.sql.tree.Table;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class WorkloadServer implements Server
{
    private boolean shutdown = true;
    private Set<String> processedQueryIds = new HashSet<>();

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
        while (shutdown == false)
        {
            System.out.println("workload is running...");
            //PrestoQueryPuller puller = new PrestoQueryPuller();
            //puller.start();
            try {
                SqlParser parser = new SqlParser();
                Object obj = HttpUtil.HttpGet(ConfigFactory.Instance().getProperty("presto.query.url"));
                JSONArray jsonArray = JSON.parseArray(obj.toString());

                for (int i = 0; i < jsonArray.size(); i++)
                {
                    JSONObject jsonObject = (JSONObject) jsonArray.get(i);
                    if (jsonObject.size() == 8)
                    {
                        // this is a valid query.
                        String queryId = jsonObject.getString("queryId");
                        if (this.processedQueryIds.contains(queryId) == false)
                        {
                            String sql = jsonObject.getString("query");
                            Query query = (Query) parser.createStatement(sql, new ParsingOptions(
                                    ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE));

                            QuerySpecification queryBody = (QuerySpecification) query.getQueryBody();
                            // get columns
                            List<SelectItem> selectItemList = queryBody.getSelect().getSelectItems();

                            // tableName
                            Table table = (Table) queryBody.getFrom().get();

                            this.processedQueryIds.add(queryId);
                        }
                    }
                }


                TimeUnit.SECONDS.sleep(1);

            } catch (InterruptedException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "error while fetching workload from presto.", e);
            }
        }
    }
}
