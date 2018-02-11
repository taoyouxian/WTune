package cn.edu.ruc.iir.rainbow.eva.cli;

import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.eva.invoker.InvokerWorkloadEvaluation;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.FileInputStream;
import java.util.Properties;

public class Main
{
    public static void main(String[] args)
    {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("Rainbow Workload Evaluation")
                .defaultHelp(true)
                .description("Evaluate seek cost of HDD-based file system.");
        parser.addArgument("-f", "--config")
                .help("specify the path of configuration file");
        parser.addArgument("-p", "--param_file").required(true)
                .help("specify the path of parameter file");

        Namespace namespace = null;
        try
        {
            namespace = parser.parseArgs(args);
        } catch (ArgumentParserException e)
        {
            parser.handleError(e);
            System.out.println("Rainbow Workload Evaluation (https://github.com/dbiir/rainbow/blob/master/rainbow-evaluate/README.md).");
            System.exit(0);
        }

        try
        {
            String configFilePath = namespace.getString("config");

            if (configFilePath != null)
            {
                ConfigFactory.Instance().loadProperties(configFilePath);
                System.out.println("System settings loaded from " + configFilePath + ".");
            } else
            {
                System.out.println("Using default system settings.");
            }

            String paramFilePath = namespace.getString("param_file");
            Invoker invoker = new InvokerWorkloadEvaluation();
            Properties params = new Properties();
            params.load(new FileInputStream(paramFilePath));
            System.out.println("Executing command WORKLOAD_EVALUATION");
            invoker.executeCommands(params);
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
