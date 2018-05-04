package cn.edu.ruc.iir.rainbow.layout.domian;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AtomicColumnletGroup extends Column
{
    private List<Columnlet> columnlets = new ArrayList<>();
    private Set<Integer> existingRowGroupIds = new HashSet<>();
    private int originColumnId = 0;

    public AtomicColumnletGroup(int columnId, Columnlet seed)
    {
        super(columnId, seed.getOriginName(), seed.getType(), seed.getSize());
        this.setOriginColumnId(seed.getColumnId());
        this.setDupId(seed.getDupId());
        this.setDuplicated(seed.isDuplicated());
        if (seed.getQueryIds().isEmpty() == false)
        {
            this.setQueryIds(new HashSet<>(seed.getQueryIds()));
        }
        this.columnlets.add(seed);
        this.existingRowGroupIds.add(seed.getRowGroupId());
    }

    public int getOriginColumnId()
    {
        return originColumnId;
    }

    protected void setOriginColumnId(int originColumnId)
    {
        this.originColumnId = originColumnId;
    }

    public boolean has (Columnlet columnlet)
    {
        if (columnlet.getColumnId() != this.getOriginColumnId())
        {
            return false;
        }
        if (this.getQueryIds().size() != columnlet.getQueryIds().size())
        {
            return false;
        }
        boolean belongs = true;
        for (int qid : this.getQueryIds())
        {
            if (columnlet.getQueryIds().contains(qid) == false)
            {
                belongs = false;
            }
        }
        return belongs;
    }

    public void addColumnlet (Columnlet columnlet)
    {
        if (columnlet.getColumnId() == this.getOriginColumnId() &&
                this.existingRowGroupIds.contains(columnlet.getRowGroupId()) == false)
        {
            this.existingRowGroupIds.add(columnlet.getRowGroupId());
            this.columnlets.add(columnlet);
            this.addSize(columnlet.getSize());
        }
    }

    public List<Columnlet> getColumnlets ()
    {
        return this.columnlets;
    }

    @Override
    public AtomicColumnletGroup clone()
    {
        AtomicColumnletGroup cloned = new AtomicColumnletGroup(this.getId(), this.columnlets.get(0));
        cloned.setDupId(this.getDupId());
        cloned.setDuplicated(this.isDuplicated());
        if (this.getQueryIds().isEmpty() == false)
        {
            cloned.setQueryIds(new HashSet<>(this.getQueryIds()));
        }
        for (int i = 1; i < this.columnlets.size(); ++i)
        {
            cloned.addColumnlet(this.columnlets.get(i));
        }
        return cloned;
    }
}
