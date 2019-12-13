package cn.edu.ruc.iir.rainbow.server;

import cn.edu.ruc.iir.rainbow.common.DateUtil;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
public class RainbowController {

    @RequestMapping("/eval")
    public String evaluate() {
        System.out.println("evaluate:" + DateUtil.formatTime(new Date()));
        return "evaluate";
    }
}
