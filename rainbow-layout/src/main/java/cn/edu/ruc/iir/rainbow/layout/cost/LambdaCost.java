package cn.edu.ruc.iir.rainbow.layout.cost;

import java.util.ArrayList;
import java.util.List;

public class LambdaCost
{
    private List<NamedCost> namedCosts = new ArrayList<>();

    public void addNamedCost(NamedCost namedCost)
    {
        this.namedCosts.add(namedCost);
    }

    public List<NamedCost> getNamedCosts()
    {
        return namedCosts;
    }

    public double calculate ()
    {
        double ms = 0;
        for (NamedCost cost : this.namedCosts)
        {
            ms += cost.getMs();
        }
        return ms;
    }
}
