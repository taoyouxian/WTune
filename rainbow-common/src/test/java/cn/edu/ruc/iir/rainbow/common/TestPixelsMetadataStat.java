package cn.edu.ruc.iir.rainbow.common;

import cn.edu.ruc.iir.rainbow.common.exception.MetadataException;
import cn.edu.ruc.iir.rainbow.common.metadata.PixelsMetadataStat;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestPixelsMetadataStat
{
    @Test
    public void test () throws IOException, MetadataException
    {
        PixelsMetadataStat stat = new PixelsMetadataStat("dbiir10", 9000,"/pixels/pixels/test_105_perf/ttt");
        double[] columnSizes = stat.getAvgColumnChunkSize();
        List<String> columnNames = stat.getFieldNames();
        for (int i = 0; i < columnNames.size(); ++i)
        {
            System.out.println(columnNames.get(i) + ", " + columnSizes[i]);
        }
        System.out.println(stat.getRowGroupCount());
    }
}
