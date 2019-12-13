package cn.edu.ruc.iir.rainbow.seek;

import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static cn.edu.ruc.iir.rainbow.seek.Constants.*;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.seek
 * @ClassName: SchemaAnalyzer
 * @Description:
 * @author: tao
 * @date: Create in 2019-12-13 20:39
 **/
public class SchemaAnalyzer {

    @Test
    public void schema() {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(schemaPath));
            BufferedWriter writer = new BufferedWriter(new FileWriter(schemaPath_Ordered));

            String[] columns = AnalyzerUtil.getLayoutFromSQL(columnPath_Ordered_Tune).split(",");

            String line;
            Map<String, String> schema = new HashMap<>();
            // schema 
            while ((line = reader.readLine()) != null) {
                String[] schemaLog = line.split("\t");
                schema.put(schemaLog[0], schemaLog[1] + "\t" + schemaLog[2]);
            }
            System.out.println("Schema number:" + schema.size());

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
