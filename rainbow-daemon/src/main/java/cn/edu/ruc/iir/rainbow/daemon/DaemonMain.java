package cn.edu.ruc.iir.rainbow.daemon;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.LogFactory;
import cn.edu.ruc.iir.rainbow.daemon.layout.LayoutServer;
import cn.edu.ruc.iir.rainbow.daemon.workload.WorkloadServer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class DaemonMain
{
    public static void main(String[] args)
    {
        if (args.length > 0)
        {
            String mainFile = ConfigFactory.Instance().getProperty("file.lock.main");
            String guardFile = ConfigFactory.Instance().getProperty("file.lock.guard");
            String daemonJarPath = ConfigFactory.Instance().getProperty("rainbow.home") +
                    ConfigFactory.Instance().getProperty("daemon.jar");

            if (args[0].equalsIgnoreCase("main"))
            {
                // this is the main daemon
                System.out.println("starting main daemon...");
                Daemon guardDaemon = new Daemon();
                String[] guardCmd = {"java", "-jar", daemonJarPath, "guard"};
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
                    String tableName = splits[0];
                    long lifeTime = Long.parseLong(splits[1]);
                    double threshold = Double.parseDouble(splits[2]);
                    container.addServer("workload", new WorkloadServer(tableName, lifeTime, threshold));
                    container.addServer("layout", new LayoutServer(tableName));
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
            } else if (args[0].equalsIgnoreCase("guard"))
            {
                // this is the guard daemon
                System.out.println("starting guard daemon...");
                Daemon guardDaemon = new Daemon();
                String[] guardCmd = {"java", "-jar", daemonJarPath, "main"};
                guardDaemon.setup(guardFile, mainFile, guardCmd);
                guardDaemon.run();
            } else if (args[0].equalsIgnoreCase("shutdown"))
            {
                System.out.println("Shutdown Daemons..");
                try
                {
                    for (int i = 1; i < args.length; ++i)
                    {
                        int pid = Integer.parseInt(args[i]);
                        System.out.println("killing " + pid);
                        Runtime.getRuntime().exec("kill -9 " + pid);
                    }
                } catch (IOException e)
                {
                    LogFactory.Instance().getLog().error("error when killing rainbow daemons.", e);
                }
            }
        }
    }
}
