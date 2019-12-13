package cn.edu.ruc.iir.rainbow.seek;

import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static cn.edu.ruc.iir.rainbow.seek.Constants.*;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.seek
 * @ClassName: QueryAnalyzer
 * @Description:
 * @author: tao
 * @date: Create in 2019-12-13 15:51
 **/
public class QueryAnalyzer {

    @Test
    public void analyze() {
        try {
            BufferedReader workloadReader = new BufferedReader(new FileReader(workloadPath));
            BufferedReader reader = new BufferedReader(new FileReader(estimateTimePath));
            BufferedWriter writer = new BufferedWriter(new FileWriter(workloadTunePath));

            String line;
            Map<String, String> query = new HashMap<>();
            // workload
            while ((line = workloadReader.readLine()) != null) {
                String[] queryLog = line.split("\t");
                query.put(queryLog[0], queryLog[2]);
            }
            System.out.println("Query number:" + query.size());

            reader.readLine();
            while ((line = reader.readLine()) != null) {
                String[] lineSplits = line.split(",");
                String value = query.get(lineSplits[0]);
                if (value != null) {
                    writer.write(lineSplits[0] + "\t1\t" + value);
                    writer.newLine();
                }
            }
            writer.flush();
            reader.close();
            writer.close();
            workloadReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void estimateTime() {
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
                writer.write(lineSplits[0] + "," + lineSplits[1] + "," + line_ordered.split(",")[1]);
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
