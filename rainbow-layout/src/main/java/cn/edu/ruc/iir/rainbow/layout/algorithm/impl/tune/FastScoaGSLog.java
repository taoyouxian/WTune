package cn.edu.ruc.iir.rainbow.layout.algorithm.impl.tune;

import cn.edu.ruc.iir.rainbow.common.exception.AlgoException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord.FastScoaGS;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.layout.algorithm.impl.tune
 * @ClassName: FastScoaGSLog
 * @Description: add log to record chosen cx & cy
 * @author: tao
 * @date: Create in 2019-12-20 11:42
 **/
public class FastScoaGSLog extends FastScoaGS {

    private double initSeekCost;
    private double scoaSeekCost;
    private String logFilePath;
    private List<Indices> indices;

    public double getInitSeekCost() {
        return initSeekCost;
    }

    public double getScoaSeekCost() {
        return scoaSeekCost;
    }

    public void setInitSeekCost(double initSeekCost) {
        this.initSeekCost = initSeekCost;
    }

    public void setInitSeekCost() {
        this.initSeekCost = super.getCurrentWorkloadSeekCost();
    }

    public void setScoaSeekCost(double scoaSeekCost) {
        this.scoaSeekCost = scoaSeekCost;
    }

    public void setup(Properties params) {
        super.setup();
        logFilePath = params.getProperty("log.file");
        indices = new ArrayList<>();
    }

    @Override
    public void runAlgorithm() {
        this.initScoaRG();
        long startSeconds = System.currentTimeMillis() / 1000;
        double executedTime = -1;
        long computationBudget = this.getComputationBudget();
        for (long currentSeconds = System.currentTimeMillis() / 1000;
             (currentSeconds - startSeconds) < this.getComputationBudget();
             currentSeconds = System.currentTimeMillis() / 1000, ++this.iterations) {
            //generate two random indices
            int i = rand.nextInt(this.getColumnOrder().size());
            int j = i;
            while (j == i)
                j = rand.nextInt(this.getColumnOrder().size());
            rand.setSeed(System.nanoTime());

            //calculate new cost
            double neighbourEnergy = getNeighbourSeekCost(i, j);

            //try to accept it
            double temperature = this.getTemperature();
            if (this.probability(currentEnergy, neighbourEnergy, temperature) > Math.random()) {
                currentEnergy = neighbourEnergy;
                updateColumnOrder(i, j);
                indices.add(new Indices(i, j));
            }

            // percentage
            if (executedTime < currentSeconds - startSeconds) {
                executedTime = currentSeconds - startSeconds;
                double temp = ((int) ((executedTime / computationBudget) * 10000) / 100.0);
                System.out.println("ORDERING: " + temp + "%");
            }
        }
        this.setScoaSeekCost(this.currentEnergy);
        System.out.println("ORDERING: 100.0%");
    }

    @Override
    public void cleanup() {
        super.cleanup();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFilePath))) {
            for (Indices i : indices) {
                writer.write(i.toString());
                writer.newLine();
            }
        } catch (IOException e) {
            ExceptionHandler.Instance().log(ExceptionType.ERROR, "I/O error, check the file paths", e);
        }
    }

    class Indices {
        private int x;
        private int y;
        private double cost; // todo record each cost

        public int getX() {
            return x;
        }

        public int getY() {
            return y;
        }

        public Indices() {
        }

        public Indices(int x, int y) {
            this.x = x;
            this.y = y;
        }

        public Indices(String x, String y) {
            this.x = Integer.parseInt(x);
            this.y = Integer.parseInt(y);
        }

        @Override
        public String toString() {
            return x +
                    "," + y;
        }
    }

    public double runAlgorithmLog() {
        this.initScoaRG();
        buildLog();
        for (Indices columnIndex : indices) {
            //generate two random indices
            int i = columnIndex.getX();
            int j = columnIndex.getY();

            //calculate new cost
            currentEnergy = getNeighbourSeekCost(i, j);
            //try to accept it
            updateColumnOrder(i, j);
        }
        this.setScoaSeekCost(this.currentEnergy);
        return currentEnergy;
    }

    public void buildLog() {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(logFilePath)));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] columnIndex = line.split(",");
                Indices inx = new Indices(columnIndex[0], columnIndex[1]);
                indices.add(inx);
            }
            reader.close();
        } catch (FileNotFoundException e) {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "log file not found.", e);
        } catch (IOException e) {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "log file close error.", e);
        }
    }

    public Object[] getIndices() {
        return indices.toArray();
    }

    public void initScoaRG() {
        try {
            long bestRowGroupSize = RowGroupSize.BestRowGroupSize(this.getNumMapSlots(),
                    this.getTotalMemory());

            while (this.getRowGroupSize() > bestRowGroupSize) {
                this.decreaseRowGroupSize();
            }
            while (this.getRowGroupSize() < bestRowGroupSize) {
                this.increaseRowGroupSize();
            }
            this.currentEnergy = super.getCurrentWorkloadSeekCost();
            this.setInitSeekCost(currentEnergy);
        } catch (AlgoException e) {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "algorithm error when running fastscoa tune with group size optimization.", e);
        }
    }

    public double getRandSeekCost(int i, int j) {
        //calculate new cost
        this.currentEnergy = getNeighbourSeekCost(i, j);
        //try to accept it
        updateColumnOrder(i, j);
        return this.currentEnergy;
    }

}
