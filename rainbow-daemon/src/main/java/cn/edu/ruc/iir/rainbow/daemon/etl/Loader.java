package cn.edu.ruc.iir.rainbow.daemon.etl;


import java.io.IOException;

public interface Loader
{
    public boolean executeLoad(String sourcePath, String orderPath, String schemaStr,
                               int[] orderMapping, int maxRowNum) throws IOException;
}
