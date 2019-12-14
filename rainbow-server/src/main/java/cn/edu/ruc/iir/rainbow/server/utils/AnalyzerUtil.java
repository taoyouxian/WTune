package cn.edu.ruc.iir.rainbow.server.utils;

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

    public static String getTime(double time, int point) {
        return time == 0 ? "0" : String.format("%." + point + "f", time);
    }

    public static String getTime(double time) {
        return time == 0 ? "0" : String.format("%.3f", time);
    }

    public static String getTime(String time, int unit, int point) {
        return getTime(Double.valueOf(time) * 1.0 / unit, point);
    }

    public static String getTime(String time, int unit) {
        return getTime(Double.valueOf(time) * 1.0 / unit);
    }

    public static double getTotalEstimatedDuration(String estimatedDurationPath, int unit) {
        double sum = 0;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(estimatedDurationPath));

            String line;
            reader.readLine();
            while ((line = reader.readLine()) != null) {
                sum += Double.valueOf(getTime(line.split(",")[1], unit));
            }

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sum;
    }

    public static void getSchemaFromLayout(String schemaPath, String schemaPath_Ordered, String[] columns) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(schemaPath));
            BufferedWriter writer = new BufferedWriter(new FileWriter(schemaPath_Ordered));

            String line;
            Map<String, String> schema = new HashMap<>();
            // schema
            while ((line = reader.readLine()) != null) {
                String[] schemaLog = line.split("\t");
                schema.put(schemaLog[0], schemaLog[1] + "\t" + schemaLog[2]);
            }

            reader.readLine();
            for (String col : columns) {
                String value = schema.get(col);
                if (value != null) {
                    writer.write(col + "\t" + value);
                    writer.newLine();
                }
            }
            writer.flush();
            reader.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
