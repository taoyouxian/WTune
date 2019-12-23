package cn.edu.ruc.iir.rainbow.server;

import cn.edu.ruc.iir.rainbow.server.vo.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;

/**
 * @author xuwen.tyx<br>
 * @version 1.0<br>
 * @description: <br>
 * @date 2019/12/22 19:20 <br>
 * @see cn.edu.ruc.iir.rainbow.server <br>
 */
@RestController
@RequestMapping("/scoa")
public class RainbowScoaController {


    @Autowired
    private ScoaService scoaService;

    @PostConstruct
    private void initSetting() {
        // todo change it to true if rescoa needed, or make it default
        scoaService.setScoaInit(false);
    }

    @RequestMapping("/init")
    @ResponseBody
    public Response<Object> init() {
        scoaService.setScoaInit(false);
        return Response.buildSucResp(scoaService.getInitSeekCost(), "init ok");
    }

    @RequestMapping("/run")
    @ResponseBody
    public Response<Object> run() {
        return Response.buildSucResp(scoaService.run());
    }

    @RequestMapping("/save")
    @ResponseBody
    public Response<Object> save() {
        return Response.buildSucResp(scoaService.save(), "save ok");
    }

    @RequestMapping("/seek")
    @ResponseBody
    public Response<Object> seek(@RequestParam("cx") int cx,
                                 @RequestParam("cy") int cy) {
        return Response.buildSucResp(scoaService.run(cx, cy));
    }

    @RequestMapping("/record")
    @ResponseBody
    public Response<Object> record() {
        return Response.buildSucResp(scoaService.getLog());
    }

    @RequestMapping("/initseek")
    @ResponseBody
    public Response<Object> initSeek() {
        return Response.buildSucResp(scoaService.getInitSeekCost());
    }

    @RequestMapping("/scoaseek")
    @ResponseBody
    public Response<Object> scoaSeek() {
        double scoaSeekCost = scoaService.getScoaSeekCost();
        return Response.buildSucResp(scoaSeekCost > 0 ? scoaSeekCost : "scoa fault, cost:" + scoaSeekCost);
    }

}
