package cn.edu.ruc.iir.rainbow.seek.service;

import cn.edu.ruc.iir.rainbow.server.ScoaService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.seek.service
 * @ClassName: ScoaServiceTest
 * @Description:
 * @author: tao
 * @date: Create in 2019-12-21 15:38
 **/
@SpringBootTest(classes = ScoaService.class)
@RunWith(SpringJUnit4ClassRunner.class)
public class ScoaServiceTest {
    @Autowired
    protected ScoaService scoaService;

    @Before
    public void init() {
        // to determine whether to run scoa or not
        scoaService.setScoaInit(false);
    }

    @Test
    public void testGetLog() {
        Object[] records = scoaService.getLog();
        Assert.assertTrue(records.length > 0);

        scoaService.initScoaRG();
        String[] record;
        double scoaSeekCost = 0;
        for (Object o : records) {
            System.out.println(o.toString());
            record = o.toString().split(",");
            scoaSeekCost = scoaService.run(record[0], record[1]);
        }
        System.out.println("cost:" + scoaSeekCost);
    }

    @Test
    public void testRun() {
        double cost = scoaService.run();
        System.out.println("cost:" + cost);
        Assert.assertTrue(cost > 0);
    }

}
