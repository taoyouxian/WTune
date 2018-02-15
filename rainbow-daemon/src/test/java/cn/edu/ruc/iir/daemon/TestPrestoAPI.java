package cn.edu.ruc.iir.daemon;

import cn.edu.ruc.iir.rainbow.common.*;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.parser.sql.parser.SqlParser;
import cn.edu.ruc.iir.rainbow.parser.sql.tree.Query;
import cn.edu.ruc.iir.rainbow.parser.sql.tree.QuerySpecification;
import cn.edu.ruc.iir.rainbow.parser.sql.tree.SelectItem;
import cn.edu.ruc.iir.rainbow.parser.sql.tree.Table;
import cn.edu.ruc.iir.rainbow.workload.cache.AccessPattern;
import cn.edu.ruc.iir.rainbow.workload.cache.AccessPatternCache;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class TestPrestoAPI
{
    int count = 0;
    private boolean flag = false;

    Map<String, Boolean> queryMap = new HashMap();

    @Test
    public void latestTest () throws IOException
    {
        SqlParser parser = new SqlParser();
        Query q = null;
        Object o = new Object();
        JSONArray jsonArray = null;

        int cou = 0;
        while (true) {
            o = HttpUtils.Instance().getPageContent(ConfigFactory.Instance().getProperty("presto.query.url"));
            jsonArray = JSON.parseArray(o.toString());

            String queryId = null;
            String query = null;
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jsonObject = (JSONObject) jsonArray.get(i);
                if (queryMap.get("queryId") != null && !queryMap.get("queryId") && jsonObject.size() == 8) {
                    queryId = jsonObject.get("queryId").toString();
                    queryMap.put("queryId", true);
                    query = jsonObject.get("query").toString();
//                    System.out.println(queryId + "\t" + i + "\t" + query);
                    // Parser
                    try {
                        q = (Query) parser.createStatement(query);
                    } catch (Exception e) {
                        ExceptionHandler.Instance().log(ExceptionType.ERROR, "query error", e);
                    }
//                System.out.println(q.toString());
                    QuerySpecification queryBody = (QuerySpecification) q.getQueryBody();
                    // get columns
                    List<SelectItem> selectItemList = queryBody.getSelect().getSelectItems();

                    // tableName
                    Table t = (Table) queryBody.getFrom().get();
//                    System.out.println(t.getName());
                    if (t.getName().toString().equals("text")) {
                        System.out.println("Text visit: " + cou++);
                        if (cou >= 4000) {
                            System.out.println(queryMap.size());
                        }
                        int j = 0;
                        Random random = new Random(System.currentTimeMillis());
                        AccessPatternCache APC = new AccessPatternCache(100000, 0.1);
                        String time = DateUtil.formatTime(new Date());
                        System.out.println(time);
                        try {
                            if (!flag) {
                                flag = true;
                                FileUtils.Instance().writeFile(time + "\tBegin\t" + i + "\r\n", Settings.APC_PATH, true);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        double weight = Double.parseDouble("1");
                        AccessPattern pattern = new AccessPattern(queryId, weight);
                        for (SelectItem column : selectItemList) {
//                        System.out.println(scolumn.toString());
                            pattern.addColumn(column.toString());
                        }
                        if (APC.cache(pattern)) {
                            System.out.println(i + ", trigger layout optimization.");
                            j++;
                            APC.saveAsWorkloadFile("/home/tao/software/station/Workplace/workload_" + j + ".txt");
                            try {
                                flag = false;
                                System.out.println(time);
                                FileUtils.Instance().writeFile(time + "\tEnd\t" + i + "\r\n", Settings.APC_PATH, true);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        try {
                            Thread.sleep(random.nextInt(500));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            o = new Object();
        }
    }

    @Test
    public void HttpGetTest() {
        SqlParser parser = new SqlParser();
        Query q = null;
        Object o = new Object();
        JSONArray jsonArray = null;

        int num = 0, cou = 0;
        while (true) {
            try {
                o = HttpUtils.Instance().getPageContent(ConfigFactory.Instance().getProperty("presto.query.url"));
            } catch (Exception e) {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "http get error", e);
            }
            jsonArray = JSON.parseArray(o.toString());

            String queryId = null;
            String query = null;
            for (int i = count; i < jsonArray.size(); i++) {
                JSONObject jsonObject = (JSONObject) jsonArray.get(i);
                if (jsonObject.size() == 8) {
                    queryId = jsonObject.get("queryId").toString();
                    query = jsonObject.get("query").toString();
//                    System.out.println(queryId + "\t" + i + "\t" + query);
                    // Parser
                    try {
                        q = (Query) parser.createStatement(query);
                    } catch (Exception e) {
                        ExceptionHandler.Instance().log(ExceptionType.ERROR, "query error", e);
                    }
//                System.out.println(q.toString());
                    QuerySpecification queryBody = (QuerySpecification) q.getQueryBody();
                    // get columns
                    List<SelectItem> selectItemList = queryBody.getSelect().getSelectItems();

                    // tableName
                    Table t = (Table) queryBody.getFrom().get();
//                    System.out.println(t.getName());
                    if (t.getName().toString().equals("text")) {
                        System.out.println("Text visit: " + cou++);
                        int j = 0;
                        Random random = new Random(System.currentTimeMillis());
                        AccessPatternCache APC = new AccessPatternCache(100000, 0.1);
                        String time = DateUtil.formatTime(new Date());
                        System.out.println(time);
                        try {
                            if (!flag) {
                                flag = true;
                                FileUtils.Instance().writeFile(time + "\tBegin\t" + i + "\r\n", Settings.APC_PATH, true);
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        double weight = Double.parseDouble("1");
                        AccessPattern pattern = new AccessPattern(queryId, weight);
                        for (SelectItem column : selectItemList) {
//                        System.out.println(scolumn.toString());
                            pattern.addColumn(column.toString());
                        }
                        if (APC.cache(pattern)) {
                            System.out.println(i + ", trigger layout optimization.");
                            j++;
                            APC.saveAsWorkloadFile("/home/tao/software/station/Workplace/workload_" + j + ".txt");
                            try {
                                flag = false;
                                System.out.println(time);
                                FileUtils.Instance().writeFile(time + "\tEnd\t" + i + "\r\n", Settings.APC_PATH, true);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        try {
                            Thread.sleep(random.nextInt(500));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            // update count
            count = jsonArray.size();
            num++;
            o = new Object();
        }
    }

    @Test
    public void HttpGetFilterTest() throws IOException
    {
        SqlParser parser = new SqlParser();
        Query q = null;
        Object o = new Object();
        JSONArray jsonArray = null;

        int num = 0, cou = 0;
        while (true) {
            o = HttpUtils.Instance().getPageContent(ConfigFactory.Instance().getProperty("presto.query.url"));
            jsonArray = JSON.parseArray(o.toString());

            String queryId = null;
            String query = null;
            for (int i = count; i < jsonArray.size(); i++) {
                System.out.println("Loop Times: " + num);
                JSONObject jsonObject = (JSONObject) jsonArray.get(i);
                if (jsonObject.size() == 8) {
                    queryId = jsonObject.get("queryId").toString();
                    query = jsonObject.get("query").toString();
//                    System.out.println(queryId + "\t" + i + "\t" + query);
                    // Parser
                    try {
                        q = (Query) parser.createStatement(query);
                    } catch (Exception e) {
                        ExceptionHandler.Instance().log(ExceptionType.ERROR, "query error", e);
                    }
//                System.out.println(q.toString());
                    QuerySpecification queryBody = (QuerySpecification) q.getQueryBody();
                    // get columns
                    List<SelectItem> selectItemList = queryBody.getSelect().getSelectItems();
                    for (SelectItem column : selectItemList) {
                        System.out.println(column.toString());
                    }
                    // tableName
                    Table t = (Table) queryBody.getFrom().get();
                    System.out.println(t.getName());
                    if (t.getName().equals("text")) {
                        System.out.println("Text visit: " + cou++);
                    }
                }
            }
            // update count
            count = jsonArray.size();
            num++;
            o = new Object();
        }
    }
}
