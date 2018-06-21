package cn.edu.ruc.iir.rainbow.layout.builder.domain;

import java.util.ArrayList;
import java.util.List;

public class OrderObj
{
    private List<String> columnOrder = new ArrayList<>();

    public List<String> getColumnOrder()
    {
        return columnOrder;
    }

    public void setColumnOrder(List<String> columnOrder)
    {
        this.columnOrder = columnOrder;
    }

    public void addColumnOrder(String column)
    {
        this.columnOrder.add(column);
    }
}
