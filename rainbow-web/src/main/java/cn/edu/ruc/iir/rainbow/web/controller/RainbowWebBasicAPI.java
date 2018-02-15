package cn.edu.ruc.iir.rainbow.web.controller;

import cn.edu.ruc.iir.rainbow.web.service.RainbowWebMain;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.SQLException;

@CrossOrigin
@Controller
public class RainbowWebBasicAPI
{


    @Autowired
    private RainbowWebMain rainbowWebMain;

    public RainbowWebBasicAPI()
    {

    }

    /**
     * Pipeline Create
     */
    @RequestMapping(value = "/schemaUpload", method = RequestMethod.POST)
    @ResponseBody
    public void schemaUpload(HttpServletRequest request, HttpServletResponse response) throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException
    {
        rainbowWebMain.schemaUpload(request, response);
    }

    @RequestMapping(value = "/getDataUrl", method = RequestMethod.GET)
    @ResponseBody
    public String getDataUrl()
    {
        return rainbowWebMain.getDataUrl();
    }

    /**
     * Pipeline List
     */
    @RequestMapping(value = "/getPipelineData", method = RequestMethod.GET)
    @ResponseBody
    public String getPipelineData() throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException
    {
        return rainbowWebMain.getPipelineData();
    }

    @RequestMapping(value = "/delete", method = RequestMethod.POST)
    @ResponseBody
    public void delete(@RequestParam(value = "pno") String arg)
    {
        rainbowWebMain.delete(arg);
    }

    @RequestMapping(value = "/stop", method = RequestMethod.POST)
    @ResponseBody
    public void stop(@RequestParam(value = "pno") String arg)
    {
        rainbowWebMain.stop(arg);
    }

    /**
     * Sampling
     */
    @RequestMapping(value = "/startSampling", method = RequestMethod.POST)
    @ResponseBody
    public void startSampling(@RequestParam(value = "pno") String arg)
    {
        rainbowWebMain.startSampling(arg);
    }

    /**
     * Workload Upload
     */
    @RequestMapping(value = "/queryUpload", method = RequestMethod.POST)
    @ResponseBody
    public void queryUpload(@RequestParam(value = "query") String arg, @RequestParam(value = "pno") String pno)
    {
        rainbowWebMain.queryUpload(arg, pno);
    }

    @RequestMapping(value = "/clientUpload", method = RequestMethod.POST)
    @ResponseBody
    public String clientUpload(@RequestParam(value = "query") String arg, @RequestParam(value = "pno") String pno, @RequestParam(value = "id") String id, @RequestParam(value = "weight") String weight)
    {
        return rainbowWebMain.clientUpload(arg, pno, id, weight);
    }

    @RequestMapping(value = "/workloadUpload", method = RequestMethod.POST)
    @ResponseBody
    public void workloadUpload(HttpServletRequest request, HttpServletResponse response) throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException
    {
        rainbowWebMain.workloadUpload(request, response);
    }

    /**
     * Layout Strategy
     */
    @RequestMapping(value = "/accept", method = RequestMethod.POST)
    @ResponseBody
    public void accept(@RequestParam(value = "pno") String arg)
    {
        rainbowWebMain.accept(arg);
    }

    @RequestMapping(value = "/optimization", method = RequestMethod.POST)
    @ResponseBody
    public void optimization(@RequestParam(value = "pno") String arg)
    {
        rainbowWebMain.optimization(arg);
    }

    @RequestMapping(value = "/getOrdered", method = RequestMethod.GET)
    @ResponseBody
    public String getOrdered(@RequestParam(value = "pno") String arg)
    {
        return rainbowWebMain.getOrdered(arg);
    }

    @RequestMapping(value = "/getEstimate_Sta", method = RequestMethod.GET)
    @ResponseBody
    public String getEstimate_Sta(@RequestParam(value = "pno") String arg)
    {
        return rainbowWebMain.getEstimate_Sta(arg);
    }

    @RequestMapping(value = "/getLayout", method = RequestMethod.GET)
    @ResponseBody
    public String getLayout(@RequestParam(value = "pno") String arg)
    {
        return rainbowWebMain.getLayout(arg);
    }

    @RequestMapping(value = "/getCurrentLayout", method = RequestMethod.GET)
    @ResponseBody
    public String getCurrentLayout(@RequestParam(value = "pno") String arg)
    {
        return rainbowWebMain.getCurrentLayout(arg);
    }

    @RequestMapping(value = "/getOrderedLayout", method = RequestMethod.GET)
    @ResponseBody
    public String getOrderedLayout(@RequestParam(value = "pno") String arg)
    {
        return rainbowWebMain.getOrderedLayout(arg);
    }

    /**
     * Evaluation
     */
    @RequestMapping(value = "/startEvaluation", method = RequestMethod.POST)
    @ResponseBody
    public void startEvaluation(@RequestParam(value = "pno") String arg) throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException
    {
        rainbowWebMain.startEvaluation(arg);
    }

    @RequestMapping(value = "/getStatistic", method = RequestMethod.GET)
    @ResponseBody
    public String getStatistic(@RequestParam(value = "pno") String arg) throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException
    {
        return rainbowWebMain.getStatistic(arg);
    }

    @RequestMapping(value = "/getQuery", method = RequestMethod.GET)
    @ResponseBody
    public String getQuery(@RequestParam(value = "rowid") String rowID, @RequestParam(value = "pno") String pno) throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException
    {
        return rainbowWebMain.getQueryByRowID(Integer.valueOf(rowID), pno);
    }

    /**
     * Pipeline Process Timeline
     */
    @RequestMapping(value = "/getProcessState", method = RequestMethod.GET)
    @ResponseBody
    public String getProcessState(@RequestParam(value = "pno") String arg) throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException
    {
        return rainbowWebMain.getProcessState(arg);
    }

    /**
     * Pipeline Process Detail
     */
    @RequestMapping(value = "/getPipelineDetail", method = RequestMethod.GET)
    @ResponseBody
    public String getPipelineDetail(@RequestParam(value = "pno") String pno, @RequestParam(value = "time") String time, @RequestParam(value = "desc") String desc) throws ClassNotFoundException, InterruptedException, IOException, ServletException, SQLException
    {
        return rainbowWebMain.getPipelineDetail(pno, time, desc);
    }

}
