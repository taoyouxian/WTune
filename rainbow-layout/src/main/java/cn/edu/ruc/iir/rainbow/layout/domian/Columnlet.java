package cn.edu.ruc.iir.rainbow.layout.domian;

import java.util.HashSet;

public class Columnlet extends Column
{
    private int numColumns = 0;
    private int rowGroupId = 0;

    public Columnlet(int rowGroupId, int columnId, int numColumns, String name, String type, double size)
    {
        super(columnId, name, type, size);
        this.rowGroupId = rowGroupId;
        this.numColumns = numColumns;
    }

    public Columnlet(int rowGroupId, int numColumns, Column column)
    {
        this(rowGroupId, column.getId(), numColumns, column.getName(),
                column.getType(), column.getSize());
    }

    public int getRowGroupId()
    {
        return rowGroupId;
    }

    public void setRowGroupId(int rowGroupId)
    {
        this.rowGroupId = rowGroupId;
    }

    public String getName()
    {
        return super.getName() + "_" + this.rowGroupId;
    }


    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Columnlet)
        {
            Columnlet c = (Columnlet) obj;
            return super.equals(c) && this.rowGroupId == c.rowGroupId;
        }
        return false;
    }

    public int getNumColumns()
    {
        return numColumns;
    }

    public int getColumnId ()
    {
        return super.getId();
    }

    @Override
    public int getId ()
    {
        return (this.rowGroupId * this.numColumns) + this.getColumnId();
    }

    @Override
    public int hashCode()
    {
        return super.hashCode() ^ this.rowGroupId;
    }

    @Override
    public int compareTo(Column c)
    {
        if (c instanceof Columnlet)
        {
            Columnlet cc = (Columnlet) c;
            if (this.rowGroupId != cc.rowGroupId)
            {
                return this.rowGroupId - cc.rowGroupId;
            } else
            {
                return this.getColumnId() - cc.getColumnId();
            }
        }
        else
        {
            return super.compareTo(c);
        }
    }

    @Override
    public Columnlet clone()
    {
        Columnlet columnlet = new Columnlet(this.getRowGroupId(), this.getColumnId(),
                this.getNumColumns(), this.getName(), this.getType(), this.getSize());
        columnlet.setDupId(this.getDupId());
        columnlet.setDuplicated(this.isDuplicated());
        if (this.getQueryIds() != null)
        {
            columnlet.setQueryIds(new HashSet<>(this.getQueryIds()));
        }
        else
        {
            columnlet.setQueryIds(null);
        }
        return columnlet;
    }
}
