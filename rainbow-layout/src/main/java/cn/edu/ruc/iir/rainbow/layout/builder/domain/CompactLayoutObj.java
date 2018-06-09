package cn.edu.ruc.iir.rainbow.layout.builder.domain;

import java.util.ArrayList;
import java.util.List;

public class CompactLayoutObj
{
    private int rowGroupNumber;
    private int columnNumber;
    private int cacheBorder;
    private List<String> columnletOrder = new ArrayList<>();

    public int getRowGroupNumber()
    {
        return rowGroupNumber;
    }

    public void setRowGroupNumber(int rowGroupNumber)
    {
        this.rowGroupNumber = rowGroupNumber;
    }

    public int getColumnNumber()
    {
        return columnNumber;
    }

    public void setColumnNumber(int columnNumber)
    {
        this.columnNumber = columnNumber;
    }

    public int getCacheBorder()
    {
        return cacheBorder;
    }

    public void setCacheBorder(int cacheBorder)
    {
        this.cacheBorder = cacheBorder;
    }

    public List<String> getColumnletOrder()
    {
        return columnletOrder;
    }

    public void setColumnletOrder(List<String> columnletOrder)
    {
        this.columnletOrder = columnletOrder;
    }

    public void addColumnletOrder(String columnlet)
    {
        this.columnletOrder.add(columnlet);
    }
}
