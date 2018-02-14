package cn.edu.ruc.iir.rainbow.layout.builder;


import cn.edu.ruc.iir.rainbow.layout.cost.RealSeekCost;
import cn.edu.ruc.iir.rainbow.layout.domian.Coordinate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hank on 2015/4/28.
 */
public class RealSeekCostBuilder
{
    private RealSeekCostBuilder() {}

    public static RealSeekCost buildFromFile(File columnOrderFile) throws IOException
    {
        BufferedReader reader = new BufferedReader(new FileReader(columnOrderFile));

        List<Coordinate> coordinates = new ArrayList<Coordinate>();

        String line;
        line = reader.readLine();
        long interval = Long.parseLong(line);
        while ((line = reader.readLine()) != null)
        {
            String[] tokens = line.split("\t");
            Coordinate coordinate = new Coordinate(Double.parseDouble(tokens[0]), Double.parseDouble(tokens[1]));
            coordinates.add(coordinate);
        }

        reader.close();

        return new RealSeekCost(interval, coordinates);
    }

    public static RealSeekCost buildFromPrometheus()
    {
        return null;
    }
}