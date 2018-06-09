package cn.edu.ruc.iir.rainbow.daemon;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.LogFactory;
import cn.edu.ruc.iir.rainbow.daemon.etl.ETLServer;
import cn.edu.ruc.iir.rainbow.daemon.layout.LayoutServer;
import cn.edu.ruc.iir.rainbow.daemon.workload.WorkloadQueue;
import cn.edu.ruc.iir.rainbow.daemon.workload.WorkloadServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

public class DaemonMain
{
    public static void main(String[] args)
    {
        String role = System.getProperty("role");

        if (role != null)
        {
            String mainFile = ConfigFactory.Instance().getProperty("file.lock.main");
            String guardFile = ConfigFactory.Instance().getProperty("file.lock.guard");
            String jarName = ConfigFactory.Instance().getProperty("daemon.jar");
            String daemonJarPath = ConfigFactory.Instance().getProperty("rainbow.home") + jarName;

            if (args.length >= 1)
            {
                mainFile += "_"+args[0];
            }

            if (role.equalsIgnoreCase("main") && args.length == 1 &&
                    (args[0].equalsIgnoreCase("workload-layout") || args[0].equalsIgnoreCase("etl")))
            {
                // this is the main daemon
                System.out.println("starting main daemon...");
                Daemon guardDaemon = new Daemon();
                String[] guardCmd = {"java", "-Drole=guard", "-jar", daemonJarPath, args[0]};
                guardDaemon.setup(mainFile, guardFile, guardCmd);
                Thread daemonThread = new Thread(guardDaemon);
                daemonThread.setName("main daemon thread");
                daemonThread.setDaemon(true);
                daemonThread.start();

                ServerContainer container = new ServerContainer();

                String[] tables = ConfigFactory.Instance().getProperty("workload.server.managed.tables").split(",");
                for (String table : tables)
                {
                    String[] splits = table.split(":");
                    String schemaTableName = splits[0];
                    String schemaName = schemaTableName.split("\\.")[0];
                    String tableName = schemaTableName.split("\\.")[1];
                    if (args[0].equalsIgnoreCase("workload-layout"))
                    {
                        long lifeTime = Long.parseLong(splits[1]);
                        double threshold = Double.parseDouble(splits[2]);
                        WorkloadQueue workloadQueue = new WorkloadQueue();
                        container.addServer("workload-" + schemaName + "." + tableName,
                                new WorkloadServer(schemaName, tableName, lifeTime, threshold, workloadQueue));
                        container.addServer("layout-" + schemaName + "." + tableName,
                                new LayoutServer(schemaName, tableName, workloadQueue));
                    }
                    else
                    {
                        container.addServer("etl", new ETLServer(tableName));
                    }
                }

                // continue the main thread
                while (true)
                {
                    try
                    {
                        for (String name : container.getServerNames())
                        {
                            if (container.chechServer(name) == false)
                            {
                                container.startServer(name);
                            }
                        }
                        TimeUnit.SECONDS.sleep(3);

                    } catch (Exception e)
                    {
                        LogFactory.Instance().getLog().error("error in the main loop of daemon.", e);
                    }
                }
            } else if (role.equalsIgnoreCase("guard") && args.length == 1 &&
                    (args[0].equalsIgnoreCase("workload-layout") || args[0].equalsIgnoreCase("etl")))
            {
                // this is the guard daemon
                System.out.println("starting guard daemon...");
                Daemon guardDaemon = new Daemon();
                String[] guardCmd = {"java", "-Drole=main", "-jar", daemonJarPath, args[0]};
                guardDaemon.setup(guardFile, mainFile, guardCmd);
                guardDaemon.run();
            } else if (role.equalsIgnoreCase("kill"))
            {
                System.out.println("Shutdown Daemons...");
                try
                {
                    Process process = Runtime.getRuntime().exec("jps -lv");
                    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    String line;
                    while ((line = reader.readLine()) != null)
                    {
                        String[] splits = line.split("\\s{1,}");
                        if (splits.length < 3)
                        {
                            continue;
                        }
                        if (splits[1].contains(jarName) &&
                                (splits[2].contains("-Drole=main") || splits[2].contains("-Drole=guard")))
                        {
                            int pid = Integer.parseInt(splits[0]);
                            System.out.println("killing " + splits[2].split("=")[1] + ", pid (" + pid + ")");
                            Runtime.getRuntime().exec("kill -9 " + pid);
                        }
                    }
                    reader.close();
                    process.destroy();
                } catch (IOException e)
                {
                    LogFactory.Instance().getLog().error("error when killing rainbow daemons.", e);
                }
            }
            else
            {
                System.err.println("Run with -Drole=[main,guard,kill], when role=main/guard, there should be an args [workload-layout/etl]");
            }
        }
        else
        {
            System.err.println("Run with -Drole=[main,guard,kill], when role=main/guard, there should be an args [workload-layout/etl]");
        }
    }
}
