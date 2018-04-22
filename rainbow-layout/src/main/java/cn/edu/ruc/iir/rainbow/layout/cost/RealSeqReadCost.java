package cn.edu.ruc.iir.rainbow.layout.cost;

import cn.edu.ruc.iir.rainbow.common.exception.CostFunctionException;
import cn.edu.ruc.iir.rainbow.layout.cost.domain.Coordinate;

import java.util.List;

public class RealSeqReadCost implements SeqReadCost
{
    private double K = 0;

    public RealSeqReadCost(long interval, List<Coordinate> points) throws CostFunctionException
    {
        if (points == null || points.size() < 1)
        {
            throw new CostFunctionException("points is null or number of points is less than 1.");
        }
        if (points.size() > 1)
        {
            double avgDeltaX = 0, avgDeltaY = 0;
            for (int i = 0; i < points.size() - 1; ++i)
            {
                Coordinate p0 = points.get(i);
                Coordinate p1 = points.get(i + 1);
                double deltaX = p1.getX() - p0.getX();
                double deltaY = p1.getY() - p0.getY();
                if (deltaX <= 0 || deltaY <= 0)
                {
                    throw new CostFunctionException("deltaX or deltaY is less than 0.");
                }
                avgDeltaX += deltaX;
                avgDeltaY += deltaY;
                avgDeltaX /= points.size() - 1;
                avgDeltaY /= points.size() - 1;
                this.K = avgDeltaY / avgDeltaX;
            }
        }
        else
        {
            this.K = points.get(0).getY() / points.get(0).getX();
        }
    }

    @Override
    public double calculate(double length)
    {
        return this.K * length;
    }
}
