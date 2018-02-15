package cn.edu.ruc.iir.rainbow.layout.domian;

import java.util.HashSet;

public class ColumnChunk extends Column
{
    private int numColumns = 0;
    private int rowGroupId = 0;

    public ColumnChunk(int rowGroupId, int columnId, int numColumns, String name, String type, double size)
    {
        super(columnId, name, type, size);
        this.rowGroupId = rowGroupId;
        this.numColumns = numColumns;
    }

    public int getRowGroupId()
    {
        return rowGroupId;
    }

    public void setRowGroupId(int rowGroupId)
    {
        this.rowGroupId = rowGroupId;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ColumnChunk)
        {
            ColumnChunk c = (ColumnChunk) obj;
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
        if (c instanceof ColumnChunk)
        {
            ColumnChunk cc = (ColumnChunk) c;
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
    public ColumnChunk clone()
    {
        ColumnChunk columnChunk = new ColumnChunk(this.getRowGroupId(), this.getColumnId(),
                this.getNumColumns(), this.getName(), this.getType(), this.getSize());
        columnChunk.setDupId(this.getDupId());
        columnChunk.setDuplicated(this.isDuplicated());
        columnChunk.setQueryIds(new HashSet<>(this.getQueryIds()));
        return columnChunk;
    }
}
