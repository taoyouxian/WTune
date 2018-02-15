package cn.edu.ruc.iir.rainbow.web.service;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.FileUtils;
import cn.edu.ruc.iir.rainbow.web.hdfs.common.SysConfig;
import cn.edu.ruc.iir.rainbow.web.hdfs.model.Statistic;
import com.alibaba.fastjson.JSON;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.web.service
 * @ClassName: RainbowWebMainTest
 * @Description:
 * @author: Tao
 * @date: Create in 2017-09-18 16:41
 **/
public class RainbowWebMainTest
{

    @Autowired
    private RainbowWebMain rainbowWebMain;

    @Test
    public void getStatistic() {
        String arg = "5ff6b2e4db4e8245bd1e809a8ec98b65";
        String filePath = SysConfig.Catalog_Project + "pipline/" + arg + "/presto_duration.csv";
        List<Statistic> list = new ArrayList<Statistic>();
        try (BufferedReader reader = FileUtils.Instance().getReader(filePath)) {
            String line;
            String[] splits;
            int i = 0;
            List<double[]> li1 = new ArrayList<double[]>();
            List<double[]> li2 = new ArrayList<double[]>();
            while ((line = reader.readLine()) != null) {
                splits = line.split(",");
                if (splits.length == 2) {
                    double[] s = {Double.valueOf(i), Double.valueOf(splits[1])};
                    li1.add(s);
                } else {
                    double[] s = {Double.valueOf(i), Double.valueOf(splits[1])};
                    double[] s1 = {Double.valueOf(i), Double.valueOf(splits[2])};
                    li1.add(s);
                    li2.add(s1);
                }
                i++;
            }
            Statistic s1, s2;
            s1 = new Statistic("Origin", li1);
            list.add(s1);
            if (li2.size() > 0) {
                s2 = new Statistic("optimization", li2);
                list.add(s2);
            } else {

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        String aJson = JSON.toJSONString(list);
        System.out.println(aJson);
    }

    @Test
    public void getHashcodeTest() {
        String str = "abc";
        System.out.println(str.hashCode());
    }

    @Test
    public void getDataUrlTest() {
        String str = ConfigFactory.Instance().getProperty("datasource");
        System.out.println(str.hashCode());
    }

    @Test
    public void getPipelineByNo() {
        rainbowWebMain.getPipelineByNo("116578a4e56f61cb50c937b2150790b5", 1);
    }


    @Test
    public void getStatisticTest() {
        String filePath = "G:\\DBIIR\\rainbow-demo\\rainbow-web\\src\\main\\resources\\pipline\\3fd97c0a9714cc7ea8d3277c535483cb\\statistic.txt";
        String queryID;
        Random r = new Random();
        int time = 0;
        for (int i = 0; i < 100; i++) {
            queryID = UUID.randomUUID().toString();
            time = r.nextInt(100);
            try {
                FileUtils.Instance().appendFile(queryID + "\t" + time, filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
