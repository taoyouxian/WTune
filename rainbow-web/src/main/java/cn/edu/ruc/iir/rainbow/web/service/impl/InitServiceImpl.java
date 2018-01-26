package cn.edu.ruc.iir.rainbow.web.service.impl;

import cn.edu.ruc.iir.rainbow.common.util.ConfigFactory;
import cn.edu.ruc.iir.rainbow.common.util.FileUtils;
import cn.edu.ruc.iir.rainbow.web.hdfs.common.SysConfig;
import cn.edu.ruc.iir.rainbow.web.hdfs.model.*;
import cn.edu.ruc.iir.rainbow.web.hdfs.model.Process;
import cn.edu.ruc.iir.rainbow.web.hdfs.util.HdfsUtil;
import cn.edu.ruc.iir.rainbow.web.service.InitServiceI;
import com.alibaba.fastjson.JSON;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;

@Service("demoInitService")
public class InitServiceImpl implements InitServiceI {

    @SuppressWarnings("unchecked")
    synchronized public void init() throws IOException {
        String path = ConfigFactory.Instance().getProperty("pipline.path");
        SysConfig.Catalog_Project = path;
        String filePath = SysConfig.Catalog_Project + "cache";
        File file = new File(filePath);
        if (!file.exists()) {
            file.mkdirs();
        }
        HdfsUtil hUtil = HdfsUtil.getHdfsUtil();
        String aJson = FileUtils.readFileToString(SysConfig.Catalog_Project + "cache/cache.txt");
        if (aJson == "" || aJson == null) {
        } else {
            SysConfig.PipelineList = JSON.parseArray(aJson,
                    Pipeline.class);
        }

        aJson = FileUtils.readFileToString(SysConfig.Catalog_Project + "cache/process.txt");
        if (aJson == "" || aJson == null) {
        } else {
            SysConfig.ProcessList = JSON.parseArray(aJson,
                    Process.class);
        }

        aJson = FileUtils.readFileToString(SysConfig.Catalog_Project + "cache/curLayout.txt");
        if (aJson == "" || aJson == null) {
        } else {
            SysConfig.CurLayout = JSON.parseArray(aJson,
                    Layout.class);
        }

        aJson = FileUtils.readFileToString(SysConfig.Catalog_Project + "cache/curEstimate.txt");
        if (aJson == "" || aJson == null) {
        } else {
            SysConfig.CurEstimate = JSON.parseArray(aJson,
                    Estimate.class);
        }

        aJson = FileUtils.readFileToString(SysConfig.Catalog_Project + "cache/orderedLayout.txt");
        if (aJson == "" || aJson == null) {
        } else {
            SysConfig.CurOrderedLayout = JSON.parseArray(aJson,
                    OrderedLayout.class);
        }
    }

    /**
     * exec after web stopped
     */
    @PreDestroy
    public void applicationEnd() {
    }
}
