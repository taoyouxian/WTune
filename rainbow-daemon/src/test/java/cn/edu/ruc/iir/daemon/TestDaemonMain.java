package cn.edu.ruc.iir.daemon;

import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.daemon.DaemonMain;
import org.junit.jupiter.api.Test;

public class TestDaemonMain
{
    @Test
    public void test()
    {
        ConfigFactory.Instance().addProperty("rainbow.home", "/home/hank/dev/idea-projects/rainbow/rainbow-daemon/target/");
        DaemonMain.main(new String[]{"main"});
    }
}
