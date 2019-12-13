package cn.edu.ruc.iir.rainbow.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan
public class RainbowHttpServer {
    public static void main(String[] args) {
        RainbowHttpServer httpServer = new RainbowHttpServer();
        httpServer.init();
        SpringApplication.run(RainbowHttpServer.class, args);
    }

    private void init() {
        System.out.println("Rainbow http server is starting...");
    }
}
