package cn.edu.ruc.iir.rainbow.layout.model.domain;

public class Layout
{
    private int id;
    private int version;
    private long createAt;
    private boolean writable;
    private boolean readable;
    private long enabledAt;
    private String initOrder;
    private String initPath;
    private String compact;
    private String compactPath;
    private String split;
    private Table table;

    public int getId()
    {
        return id;
    }

    public void setId(int id)
    {
        this.id = id;
    }

    public int getVersion()
    {
        return version;
    }

    public void setVersion(int version)
    {
        this.version = version;
    }

    public long getCreateAt()
    {
        return createAt;
    }

    public void setCreateAt(long createAt)
    {
        this.createAt = createAt;
    }

    public boolean isWritable()
    {
        return writable;
    }

    public void setWritable(boolean writable)
    {
        this.writable = writable;
    }

    public boolean isReadable()
    {
        return readable;
    }

    public void setReadable(boolean readable)
    {
        this.readable = readable;
    }

    public long getEnabledAt()
    {
        return enabledAt;
    }

    public void setEnabledAt(long enabledAt)
    {
        this.enabledAt = enabledAt;
    }

    public String getInitOrder()
    {
        return initOrder;
    }

    public void setInitOrder(String initOrder)
    {
        this.initOrder = initOrder;
    }

    public String getInitPath()
    {
        return initPath;
    }

    public void setInitPath(String initPath)
    {
        this.initPath = initPath;
    }

    public String getCompact()
    {
        return compact;
    }

    public void setCompact(String compact)
    {
        this.compact = compact;
    }

    public String getCompactPath()
    {
        return compactPath;
    }

    public void setCompactPath(String compactPath)
    {
        this.compactPath = compactPath;
    }

    public String getSplit()
    {
        return split;
    }

    public void setSplit(String split)
    {
        this.split = split;
    }

    public Table getTable()
    {
        return table;
    }

    public void setTable(Table table)
    {
        this.table = table;
    }

    @Override
    public int hashCode()
    {
        return this.id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o instanceof Layout)
        {
            return this.id == ((Layout) o).id;
        }
        return false;
    }
}
