package cn.edu.ruc.iir.rainbow.server;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class HttpController {
    @RequestMapping("/")
    public String index() {
        return "index.html";
    }
}
