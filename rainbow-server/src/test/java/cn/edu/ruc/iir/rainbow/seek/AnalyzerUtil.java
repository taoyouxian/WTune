package cn.edu.ruc.iir.rainbow.seek;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.seek
 * @ClassName: AnalyzerUtil
 * @Description:
 * @author: tao
 * @date: Create in 2019-12-13 19:13
 **/
public class AnalyzerUtil {

    public static String getLayoutFromSQL(String layoutPath) {
        String columns = null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(layoutPath));

            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("Column_")) {
                    sb.append(line.split(" ")[0]).append(",");
                }
            }
            columns = sb.toString();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return columns.substring(0, columns.length() - 1);
    }

    public static void joinEstimatedDuration(String estimateTimePath, String estimateTimePath_Ordered, String workloadCostPath, boolean id) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(estimateTimePath));
            BufferedReader reader_ordered = new BufferedReader(new FileReader(estimateTimePath_Ordered));
            BufferedWriter writer = new BufferedWriter(new FileWriter(workloadCostPath));

            String line;
            String line_ordered;
            Map<String, String> query = new HashMap<>();
            reader.readLine();
            reader_ordered.readLine();
            while ((line = reader.readLine()) != null) {
                String[] lineSplits = line.split(",");
                line_ordered = reader_ordered.readLine();
                if (id)
                    writer.write(lineSplits[0] + ",");
                writer.write(lineSplits[1] + "," + line_ordered.split(",")[1]);
                writer.newLine();
            }

            writer.flush();
            reader.close();
            reader_ordered.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
