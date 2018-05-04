package cn.edu.ruc.iir.rainbow.common;

import cn.edu.ruc.iir.rainbow.common.exception.MetadataException;
import cn.edu.ruc.iir.rainbow.common.metadata.ParquetMetadataStat;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class TestParquetMetadata
{
    @Test
    public void testGetMetadata () throws IOException, MetadataException
    {
        //ParquetMetadataStat stat = new ParquetMetadataStat("10.172.96.77", 9000, "/lineitem_grouped_test_parq");
        ParquetMetadataStat stat = new ParquetMetadataStat("10.77.40.236", 9000, "/dataset/105_parq");
        double[] avgs = stat.getAvgColumnChunkSize();
        double[] devs = stat.getColumnChunkSizeStdDev(avgs);
        List<String> names = stat.getFieldNames();
        System.out.println(stat.getFileCount() + ", " + stat.getRowGroupCount());

        for (int i = 0; i < avgs.length; ++i)
        {
            System.out.println(names.get(i) + '\t' + avgs[i]);
        }
        System.out.println(stat.getTotalSize());
    }
}
