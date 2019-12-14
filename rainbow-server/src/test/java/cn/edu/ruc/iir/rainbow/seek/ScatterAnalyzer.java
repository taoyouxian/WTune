package cn.edu.ruc.iir.rainbow.seek;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static cn.edu.ruc.iir.rainbow.seek.AnalyzerUtil.getTime;

/**
 * @author xuwen.tyx<br>
 * @version 1.0<br>
 * @description: <br>
 * @date 2019/12/13 23:40 <br>
 * @see cn.edu.ruc.iir.rainbow.seek <br>
 */
public class ScatterAnalyzer {

    @Test
    /**
     * https://www.echartsjs.com/examples/zh/editor.html
     * chart/template_scatter.txt, copy the content to the above url
     */
    public void scatter() {
        String path = ConfigFactory.Instance().getProperty("tune.path");
        String estimatedDuration = path + "rl/estimate_duration.csv";
        String estimatedDuration_Ordered = path + "rl/estimate_duration_ordered.csv";
        String joinEstimatedDuration = path + "rl/estimate_duration_joined_id.csv";

        // join two estimated duration
        AnalyzerUtil.joinEstimatedDuration(estimatedDuration, estimatedDuration_Ordered, joinEstimatedDuration, true);

        try {
            BufferedReader reader = new BufferedReader(new FileReader(joinEstimatedDuration));

            int count = 0;
            int stop = 10;
            String line;
            StringBuilder sb = new StringBuilder();
            StringBuilder sb_ordered = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                String[] lineSplits = line.split(",");
                sb.append("[").append(count).append(",").append(getTime(lineSplits[1], 1000)).append("],");
                sb_ordered.append("[").append(count).append(",").append(getTime(lineSplits[2], 1000)).append("],");
                count++;
                if (count > stop) {
                    break;
                }
            }
            System.out.println("series_date:" + sb.toString());
            System.out.println("series_date_ordered:" + sb_ordered.toString());

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}