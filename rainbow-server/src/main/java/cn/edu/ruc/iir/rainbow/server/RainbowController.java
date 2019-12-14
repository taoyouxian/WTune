package cn.edu.ruc.iir.rainbow.server;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.DateUtil;
import cn.edu.ruc.iir.rainbow.common.FileUtils;
import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.InvokerException;
import cn.edu.ruc.iir.rainbow.layout.invoker.InvokerPERFESTIMATION;
import cn.edu.ruc.iir.rainbow.server.utils.AnalyzerUtil;
import cn.edu.ruc.iir.rainbow.server.vo.Response;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@RestController
public class RainbowController {
    // daemon
    private ConcurrentHashMap<String, AtomicLong> daemonLog;
    private AtomicLong whichId = new AtomicLong(0);
    private double daemonCost;
    private String layout;

    // task id
    private String tunePath;
    private String daemonPath;
    private String[] curLayout;
    private double lowCost;

    // default
    private String default_workload;
    private String default_schema;

    @PostConstruct
    private void initSetting() {
        daemonLog = new ConcurrentHashMap<>();
        tunePath = ConfigFactory.Instance().getProperty("tune.path");
        lowCost = Double.parseDouble(ConfigFactory.Instance().getProperty("tune.cost"));
        curLayout = FileUtils.readFile(tunePath + "tune/column.txt").split(",");
        layout = FileUtils.readFile(tunePath + "tune/column.txt");

        default_workload = tunePath + "tune/workload.txt";
        default_schema = tunePath + "tune/schema.txt";

        daemonCost = lowCost;
    }

    @RequestMapping("/eval")
    @ResponseBody
    public Response<Object> evaluate() {
        System.out.println("evaluate:" + DateUtil.formatTime(new Date()));
        String uuid = UUID.randomUUID().toString();
        System.out.println(uuid + "\n");
        return Response.buildSucResp(uuid);
    }

    @RequestMapping("/lowCost")
    @ResponseBody
    public Response<Object> lowCost() {
        return Response.buildSucResp(lowCost);
    }

    @RequestMapping("/layout")
    @ResponseBody
    public Response<Object> layout() {
        return Response.buildSucResp(layout.replace("Column", ""));
    }

    @RequestMapping("/daemon")
    @ResponseBody
    public Response<Object> daemon() {
        // todo daemon
        return Response.buildSucResp("ok");
    }

    @RequestMapping("/seek")
    @ResponseBody
    public Response<Object> estimated_seek(@RequestParam("id") String id,
                                           @RequestParam("layout") String layout) {
        double cost;
        if (!daemonLog.contains(id)) {
            daemonLog.put(id, whichId);
        }

        daemonPath = tunePath + id + "/" + daemonLog.get(id).getAndAdd(1) + "/";
        FileUtils.mkdir(daemonPath);

        if (layout.equals("")) {
            List<String> temp = Arrays.asList(curLayout);
            Collections.shuffle(temp);
            temp.toArray(curLayout);
        } else {
            layout = layout.replace("_", "Column_");
            curLayout = layout.split(",");
        }
        String cur_estimated_duration = daemonPath + "estimate_duration.csv";
        String cur_schema = daemonPath + "schema.txt";
        AnalyzerUtil.getSchemaFromLayout(default_schema, cur_schema, curLayout);

        cost = getEstimationTotal(cur_estimated_duration, cur_schema);
        if (cost != 0 && cost < daemonCost) {
            System.out.println(id + "-" + (daemonLog.get(id).longValue() - 1) + ">total cost:" + cost);
            daemonCost = cost;
        } else {
            FileUtils.deleteDir(daemonPath);
        }
        return Response.buildSucResp(cost);
    }

    public double getEstimationTotal(String estimated_duration, String cur_schema) {
        Invoker invoker = new InvokerPERFESTIMATION();
        Properties params = new Properties();
        params.setProperty("workload.file", default_workload);
        params.setProperty("schema.file", cur_schema);
        params.setProperty("log.file", estimated_duration);
        params.setProperty("num.row.group", "128");
        params.setProperty("seek.cost.function", "power");
        try {
            invoker.executeCommands(params);
        } catch (InvokerException e) {
            e.printStackTrace();
        }
        return AnalyzerUtil.getTotalEstimatedDuration(estimated_duration, 1000);
    }

}
