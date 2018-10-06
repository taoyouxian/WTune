package cn.edu.ruc.iir.rainbow.eva.cmd;

import cn.edu.ruc.iir.rainbow.common.HttpUtils;
import cn.edu.ruc.iir.rainbow.common.Settings;
import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.ProgressListener;
import cn.edu.ruc.iir.rainbow.common.cmd.Receiver;
import cn.edu.ruc.iir.rainbow.common.exception.*;
import cn.edu.ruc.iir.rainbow.common.metadata.OrcMetadataStat;
import cn.edu.ruc.iir.rainbow.common.metadata.ParquetMetadataStat;
import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.eva.LocalParquetEvaluator;
import cn.edu.ruc.iir.rainbow.eva.PrestoEvaluator;
import cn.edu.ruc.iir.rainbow.eva.SparkV1Evaluator;
import cn.edu.ruc.iir.rainbow.eva.SparkV2Evaluator;
import cn.edu.ruc.iir.rainbow.eva.domain.Column;
import cn.edu.ruc.iir.rainbow.eva.metrics.LocalMetrics;
import cn.edu.ruc.iir.rainbow.eva.metrics.StageMetrics;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import parquet.hadoop.metadata.ParquetMetadata;

import java.io.*;
import java.util.*;

/**
 * Created by hank on 17-5-4.
 */
public class CmdWorkloadEva implements Command
{
    private Receiver receiver = null;

    @Override
    public void setReceiver(Receiver receiver)
    {
        this.receiver = receiver;
    }

    /**
     * params should contain the following settings:
     * <ol>
     *   <li>method, LOCAL, SPARK1 or SPARK2</li>
     *   <li>format, PARQUET or ORC</li>
     *   <li>table.dir, the path of table directory on HDFS</li>
     *   <li>table.name, the table name for presto, only needed when PRESTO method is used.</li>
     *   <li>workload.file workload file path</li>
     *   <li>log.dir the local directory used to write evaluation results, must end with '/'</li>
     *   <li>drop.cache, true or false, whether or not drop file cache on each node in the cluster</li>
     *   <li>drop.caches.sh, the file path of drop_caches.sh</li>
     *   <li>pipeline.no, the pipeline number for rainbow-web, only needed when RAINBOW-WEB method is used.</li>
     * </ol>
     *
     * this method will pass the following results to receiver:
     * <ol>
     *   <li>log.dir</li>
     *   <li>success, true or false</li>
     * </ol>
     * @param params
     */
    @SuppressWarnings("Duplicates")
    @Override
    public void execute(Properties params)
    {
        Properties results = new Properties(params);
        results.setProperty("success", "false");
        ProgressListener progressListener = percentage -> {
            if (receiver != null)
            {
                receiver.progress(percentage);
            }
        };
        progressListener.setPercentage(0.0);

        String tablePath = params.getProperty("table.dir");
        String workloadFilePath = params.getProperty("workload.file");
        String logDir = params.getProperty("log.dir");
        boolean dropCache = Boolean.parseBoolean(params.getProperty("drop.cache"));
        String dropCachesSh = params.getProperty("drop.caches.sh");
        double workloadFileLength = (new File(workloadFilePath)).length();
        double readLength = 0;

        if (!logDir.endsWith("/"))
        {
            logDir += "/";
        }
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName());
        if (params.getProperty("method").equalsIgnoreCase("LOCAL"))
        {
            if (!params.getProperty("format").equalsIgnoreCase("PARQUET"))
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "local workload evaluation error.",
                        new NotSupportedException(params.getProperty("format") + " format not supported"));
                if (receiver != null)
                {
                    receiver.action(results);
                }
                return;
            }

            try (BufferedReader workloadReader = new BufferedReader(new FileReader(workloadFilePath));
                 BufferedWriter timeWriter = new BufferedWriter(new FileWriter(logDir + "local_duration.csv"));
                 BufferedWriter columnWriter = new BufferedWriter(new FileWriter(logDir + "accessed_columns.txt")))
            {
                // get metadata
                FileStatus[] statuses = LocalParquetEvaluator.getFileStatuses("hdfs://" + ConfigFactory.Instance().getProperty("namenode.host") + ":" +
                        ConfigFactory.Instance().getProperty("namenode.port") + tablePath, conf);
                ParquetMetadata[] metadatas = LocalParquetEvaluator.getMetadatas(statuses, conf);

                timeWriter.write("\"query id\",\"duration(ms)\"\n");
                columnWriter.write("# Column index and name of accessed columns of each query in Parquet metadata.\n");
                timeWriter.flush();
                columnWriter.flush();

                String line;
                while ((line = workloadReader.readLine()) != null)
                {
                    readLength += line.length();
                    String columns = line.split("\t")[2];
                    String queryId = line.split("\t")[0];
                    // evaluate
                    // clear the caches and buffers
                    if (dropCache)
                    {
                        Runtime.getRuntime().exec(dropCachesSh);
                    }
                    LocalMetrics metrics = LocalParquetEvaluator.execute(statuses, metadatas, columns.split(","), conf);

                    // log the results
                    timeWriter.write(queryId + "," + metrics.getTimeMillis() + "\n");
                    timeWriter.flush();
                    columnWriter.write("[query " + queryId + "]:\n");
                    for (Column column : metrics.getColumns())
                    {
                        columnWriter.write(column.getIndex() + "," + column.getName() + "\n");
                    }
                    columnWriter.write("\n\n");
                    columnWriter.flush();
                    progressListener.setPercentage(readLength/workloadFileLength);
                }

                results.setProperty("success", "true");
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "evaluate local error", e);
            }
        }
        else if (params.getProperty("method").equalsIgnoreCase("SPARK1") ||
                params.getProperty("method").equalsIgnoreCase("SPARK2"))
        {
            String sparkMaster = ConfigFactory.Instance().getProperty("spark.master");
            String namenodeHost = ConfigFactory.Instance().getProperty("namenode.host");
            int namenodePort = Integer.valueOf(ConfigFactory.Instance().getProperty("namenode.port"));
            int appPort = Integer.parseInt(ConfigFactory.Instance().getProperty("spark.app.port"));
            int driverWebappsPort = Integer.parseInt(ConfigFactory.Instance().getProperty("spark.driver.webapps.port"));
            try (BufferedReader workloadReader = new BufferedReader(new FileReader(workloadFilePath));
                 BufferedWriter timeWriter = new BufferedWriter(new FileWriter(logDir + "spark_duration.csv")))
            {
                // get the column sizes
                Map<String, Double> nameSizeMap = new HashMap<>();
                if (params.getProperty("format").equalsIgnoreCase("ORC"))
                {
                    if (params.getProperty("method").equalsIgnoreCase("SPARK1"))
                    {
                        ExceptionHandler.Instance().log(ExceptionType.ERROR, "Spark workload evaluation error.",
                                new NotSupportedException("ORC format not supported for Spark1"));
                        if (receiver != null)
                        {
                            receiver.action(results);
                        }
                        return;
                    }
                    OrcMetadataStat stat = new OrcMetadataStat(namenodeHost, namenodePort, tablePath);
                    int n = stat.getFieldNames().size();
                    List<String> names = stat.getFieldNames();
                    double[] sizes = stat.getAvgColumnChunkSize();

                    for (int j = 0; j < n; ++j)
                    {
                        nameSizeMap.put(names.get(j).toLowerCase(), sizes[j]);
                    }
                }
                else if (params.getProperty("format").equalsIgnoreCase("PARQUET"))
                {
                    ParquetMetadataStat stat = new ParquetMetadataStat(namenodeHost, namenodePort, tablePath);
                    int n = stat.getFieldNames().size();
                    List<String> names = stat.getFieldNames();
                    double[] sizes = stat.getAvgColumnChunkSize();

                    for (int j = 0; j < n; ++j)
                    {
                        nameSizeMap.put(names.get(j).toLowerCase(), sizes[j]);
                    }
                }
                else
                {
                    ExceptionHandler.Instance().log(ExceptionType.ERROR, "Spark workload evaluation error.",
                            new NotSupportedException(params.getProperty("format") + " format not supported"));
                    if (receiver != null)
                    {
                        receiver.action(results);
                    }
                    return;
                }

                timeWriter.write("\"query id\",\"duration(ms)\"\n");
                timeWriter.flush();

                // begin evaluate
                String line;
                int i = 0;
                while ((line = workloadReader.readLine()) != null)
                {
                    readLength += line.length();
                    String columns = line.split("\t")[2];
                    String queryId = line.split("\t")[0];

                    // get the smallest column as the order by column
                    String orderByColumn = null;
                    double size = Double.MAX_VALUE;

                    for (String name : columns.split(","))
                    {
                        if (nameSizeMap.get(name.toLowerCase()) < size)
                        {
                            size = nameSizeMap.get(name.toLowerCase());
                            orderByColumn = name.toLowerCase();
                        }
                    }

                    // evaluate
                    // clear the caches and buffers
                    if (dropCache)
                    {
                        Runtime.getRuntime().exec(dropCachesSh);
                    }

                    StageMetrics metrics = null;
                    if (params.getProperty("method").equalsIgnoreCase("SPARK1"))
                    {
                        metrics = SparkV1Evaluator.execute("rainbow_" + (i++) + "_" + queryId,
                                sparkMaster, appPort, driverWebappsPort,
                                ConfigFactory.Instance().getProperty("spark.warehouse.dir"),
                                Integer.parseInt(ConfigFactory.Instance().getProperty("spark.executor.cores")),
                                ConfigFactory.Instance().getProperty("spark.executor.memory"),
                                "hdfs://" + ConfigFactory.Instance().getProperty("namenode.host") + ":" +
                                        ConfigFactory.Instance().getProperty("namenode.port") + tablePath,
                                columns, orderByColumn);
                    }
                    else
                    {
                        metrics = SparkV2Evaluator.execute("rainbow_" + (i++) + "_" + queryId,
                                sparkMaster, appPort, driverWebappsPort,
                                ConfigFactory.Instance().getProperty("spark.warehouse.dir"),
                                Integer.parseInt(ConfigFactory.Instance().getProperty("spark.executor.cores")),
                                ConfigFactory.Instance().getProperty("spark.executor.memory"),
                                params.getProperty("format"),
                                "hdfs://" + ConfigFactory.Instance().getProperty("namenode.host") + ":" +
                                        ConfigFactory.Instance().getProperty("namenode.port") + tablePath,
                                columns, orderByColumn);
                        if (params.getProperty("format").equalsIgnoreCase("ORC"))
                        {
                            File hiveLocalMetaStorePath = new File("metastore_db");
                            try
                            {
                                FileUtils.deleteDirectory(hiveLocalMetaStorePath);
                            } catch (IOException e)
                            {
                                ExceptionHandler.Instance().log(ExceptionType.ERROR, "delete hive local metastore error", e);
                            }
                        }

                    }

                    // log the results
                    timeWriter.write(queryId + "," + metrics.getDuration() + "\n");
                    timeWriter.flush();
                    progressListener.setPercentage(readLength/workloadFileLength);
                }
                results.setProperty("success", "true");

            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "evaluate Spark i/o error", e);
            } catch (MetadataException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "evaluate Spark metadata error", e);
            }


        }
        else if (params.getProperty("method").equalsIgnoreCase("PRESTO"))
        {
            String namenodeHost = ConfigFactory.Instance().getProperty("namenode.host");
            int namenodePort = Integer.valueOf(ConfigFactory.Instance().getProperty("namenode.port"));
            try (BufferedReader workloadReader = new BufferedReader(new FileReader(workloadFilePath));
                 BufferedWriter timeWriter = new BufferedWriter(new FileWriter(logDir + "presto_duration.csv")))
            {
                // get the column sizes
                Map<String, Double> nameSizeMap = new HashMap<>();
                if (params.getProperty("format").equalsIgnoreCase("ORC"))
                {
                    OrcMetadataStat stat = new OrcMetadataStat(namenodeHost, namenodePort, tablePath);
                    int n = stat.getFieldNames().size();
                    List<String> names = stat.getFieldNames();
                    double[] sizes = stat.getAvgColumnChunkSize();

                    for (int j = 0; j < n; ++j)
                    {
                        nameSizeMap.put(names.get(j).toLowerCase(), sizes[j]);
                    }
                }
                else if (params.getProperty("format").equalsIgnoreCase("PARQUET"))
                {
                    ParquetMetadataStat stat = new ParquetMetadataStat(namenodeHost, namenodePort, tablePath);
                    int n = stat.getFieldNames().size();
                    List<String> names = stat.getFieldNames();
                    double[] sizes = stat.getAvgColumnChunkSize();

                    for (int j = 0; j < n; ++j)
                    {
                        nameSizeMap.put(names.get(j).toLowerCase(), sizes[j]);
                    }
                }
                else
                {
                    ExceptionHandler.Instance().log(ExceptionType.ERROR, "Presto workload evaluation error.",
                            new NotSupportedException(params.getProperty("format") + " format not supported"));
                    if (receiver != null)
                    {
                        receiver.action(results);
                    }
                    return;
                }
                timeWriter.write("\"query id\",\"duration(ms)\"\n");
                timeWriter.flush();

                try
                {
                    Class.forName("com.facebook.presto.jdbc.PrestoDriver");
                } catch (ClassNotFoundException e)
                {
                    ExceptionHandler.Instance().log(ExceptionType.ERROR, "evaluate Presto get JDBC driver error", e);
                }

                // begin evaluate
                String line;
                int i = 0;
                while ((line = workloadReader.readLine()) != null)
                {
                    readLength += line.length();
                    String columns = line.split("\t")[2];
                    String queryId = line.split("\t")[0];

                    // get the smallest column as the order by column
                    String orderByColumn = null;
                    double size = Double.MAX_VALUE;

                    for (String name : columns.split(","))
                    {
                        if (nameSizeMap.get(name.toLowerCase()) < size)
                        {
                            size = nameSizeMap.get(name.toLowerCase());
                            orderByColumn = name.toLowerCase();
                        }
                    }

                    // evaluate
                    // clear the caches and buffers
                    if (dropCache)
                    {
                        Runtime.getRuntime().exec(dropCachesSh);
                    }

                    StageMetrics metrics = PrestoEvaluator.execute(params.getProperty("table.name"), columns, orderByColumn);

                    // log the results
                    timeWriter.write(queryId + "," + metrics.getDuration() + "\n");
                    timeWriter.flush();
                    progressListener.setPercentage(readLength/workloadFileLength);
                }
                results.setProperty("success", "true");
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "evaluate Presto i/o error", e);
            } catch (MetadataException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "evaluate Presto metadata error", e);
            }
        }
        else if (params.getProperty("method").equalsIgnoreCase("RAINBOW-WEB"))
        {
            String pno = params.getProperty("pipeline.no");

            Random random = new Random(System.currentTimeMillis());
            try (BufferedReader reader = new BufferedReader(new FileReader(workloadFilePath));)
            {
                String line = null;
                while ((line = reader.readLine()) != null)
                {
                    String[] tokens = line.split("\t");
                    double weight = Double.parseDouble(tokens[1]);
                    String aPostData = "query=" + tokens[2] + "&pno=" + pno + "&id=" + tokens[0] + "&weight=" + weight;
                    String res = HttpUtils.Instance().acHttpPost(Settings.WORKLOAD_POST_URL, aPostData).toString();
                    System.out.println("response of qid [" + tokens[0] + "]: " + res);
                    Thread.sleep(random.nextInt(500));
                }
            } catch (IOException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "evaluate rainbow-web i/o error", e);
            } catch (InterruptedException e)
            {
                ExceptionHandler.Instance().log(ExceptionType.ERROR, "evaluate rainbow-web interrupted error", e);
            }
        }
        else
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "evaluate rainbow-web params error",
                    new CommandException("method=" + params.getProperty("method") + "is not expected."));
        }

        if (receiver != null)
        {
            receiver.action(results);
        }
    }
}
