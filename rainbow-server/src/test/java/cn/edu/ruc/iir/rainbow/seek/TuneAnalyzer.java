package cn.edu.ruc.iir.rainbow.seek;

import cn.edu.ruc.iir.rainbow.common.FileUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static cn.edu.ruc.iir.rainbow.seek.Constants.*;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.seek
 * @ClassName: TuneAnalyzer
 * @Description:
 * @author: tao
 * @date: Create in 2019-12-13 19:11
 **/
public class TuneAnalyzer {

    @Test
    public void getColumn() throws IOException {
        String columns = AnalyzerUtil.getLayoutFromSQL(ddlPath);
        System.out.println(columns);
        FileUtils.writeFile(columns, columnPath_Tune);

        String columns_ordered = AnalyzerUtil.getLayoutFromSQL(ddlPath_Ordered);
        System.out.println(columns_ordered);
        FileUtils.writeFile(columns_ordered, columnPath_Ordered_Tune);
    }

    @Test
    public void column() throws IOException {
        try {
            BufferedReader workloadReader = new BufferedReader(new FileReader(default_WorkloadPath));

            int[] colnum = new int[1000];
            String line;
            // workload
            int linenum = 0;
            int max = 0;
            String maxQ = null;
            while ((line = workloadReader.readLine()) != null) {
                String query = line.split("\t")[2];
                String[] cols = query.split(",");
                if (cols.length > max) {
                    max = cols.length;
                    maxQ = query;
                }
                for (String col : cols) {
                    colnum[Integer.parseInt(col.split("_")[1]) - 1]++;
                }
                linenum++;
            }

            int count = 0;
            for (int i = 0; i < colnum.length; i++) {
                if (colnum[i] == 0) {
                    System.out.println("Column_" + (i + 1));
                    count++;
                }
            }
            System.out.println("Not exist count:" + count);
            System.out.println("Query count:" + linenum);
            System.out.println("Longest query:" + maxQ);
            System.out.println("Longest query's len:" + max);

            workloadReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
