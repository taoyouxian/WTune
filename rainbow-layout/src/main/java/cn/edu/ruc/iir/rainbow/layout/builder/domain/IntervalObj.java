package cn.edu.ruc.iir.rainbow.layout.builder.domain;

import java.util.ArrayList;
import java.util.List;

public class IntervalObj
{
    private String status;
    private Data data;

    public String getStatus()
    {
        return status;
    }

    public void setStatus(String status)
    {
        this.status = status;
    }

    public Data getData()
    {
        return data;
    }

    public void setData(Data data)
    {
        this.data = data;
    }

    public static class Data
    {
        private String resultType;
        private List<Result> result = new ArrayList<>();

        public String getResultType()
        {
            return resultType;
        }

        public void setResultType(String resultType)
        {
            this.resultType = resultType;
        }

        public List<Result> getResult()
        {
            return result;
        }

        public void setResult(List<Result> result)
        {
            this.result = result;
        }

        public void addResult(Result result)
        {
            this.result.add(result);
        }
    }

    public static class Result
    {
        private Metric metric;
        private List<Double> value = new ArrayList<>();

        public Metric getMetric()
        {
            return metric;
        }

        public void setMetric(Metric metric)
        {
            this.metric = metric;
        }

        public List<Double> getValue()
        {
            return value;
        }

        public void setValue(List<Double> value)
        {
            this.value = value;
        }

        public void addValue(double value)
        {
            this.value.add(value);
        }
    }

    public static class Metric
    {
        private String job;

        public String getJob()
        {
            return job;
        }

        public void setJob(String job)
        {
            this.job = job;
        }
    }
}
