package cn.edu.ruc.iir.rainbow.common;

import org.junit.Test;

/**
 * Created by hank on 17-5-4.
 */
public class TestConfigFactory
{
    @Test
    void testGetProperty()
    {
        System.out.println(ConfigFactory.Instance().getProperty("refine"));
    }
}
