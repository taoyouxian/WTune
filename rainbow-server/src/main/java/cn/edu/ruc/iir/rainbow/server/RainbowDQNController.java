package cn.edu.ruc.iir.rainbow.server;

import cn.edu.ruc.iir.rainbow.common.DateUtil;
import cn.edu.ruc.iir.rainbow.server.vo.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.util.Date;
import java.util.UUID;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.server
 * @ClassName: RainbowDQNController
 * @Description:
 * @author: tao
 * @date: Create in 2019-12-18 20:51
 **/

@RestController
@RequestMapping("/tune")
public class RainbowDQNController {

    @Autowired
    private DQNService dqnService;

    @PostConstruct
    private void initSetting() {

    }

    @RequestMapping("/dqn")
    @ResponseBody
    public Response<Object> dqn() {
        System.out.println("dqn:" + DateUtil.formatTime(new Date()));
        String uuid = UUID.randomUUID().toString();
        System.out.println(uuid + "\n");
        return Response.buildSucResp(uuid);
    }

    @RequestMapping("/seek")
    @ResponseBody
    public Response<Object> seek(@RequestParam("cx") int cx,
                                 @RequestParam("cy") int cy) {
        return Response.buildSucResp(dqnService.run(cx, cy));
    }

    @RequestMapping("/cost")
    @ResponseBody
    public double cost(@RequestParam("cx") int cx,
                       @RequestParam("cy") int cy) {
        return dqnService.run(cx, cy);
    }

    @RequestMapping("/layout")
    @ResponseBody
    public Response<Object> layout() {
        // todo get tune layout, means existing columns in the workload
        return Response.buildSucResp("");
    }

    @RequestMapping("/order")
    @ResponseBody
    public String order() {
        String order = dqnService.getColumnOrder();
        return order.replace("Column_", "");
    }

    @RequestMapping("/init")
    @ResponseBody
    public Response<Object> init() {
        dqnService.initAlgoConfig();
        return Response.buildSucResp(dqnService.getInitSeekCost(), "init");
    }

    @RequestMapping("/current")
    @ResponseBody
    public Response<Object> current() {
        return Response.buildSucResp(dqnService.getCurrentSeekCost(), "current");
    }

}
