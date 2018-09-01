package cn.edu.ruc.iir.rainbow.layout.model.dao;

import java.util.List;

public interface Dao<T>
{
    public T getById(int id);

    public List<T> getAll();
}
