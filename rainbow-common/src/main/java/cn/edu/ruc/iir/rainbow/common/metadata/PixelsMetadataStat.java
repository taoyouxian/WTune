package cn.edu.ruc.iir.rainbow.common.metadata;

import cn.edu.ruc.iir.pixels.core.PixelsReader;
import cn.edu.ruc.iir.pixels.core.PixelsReaderImpl;
import cn.edu.ruc.iir.rainbow.common.exception.MetadataException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class PixelsMetadataStat implements MetadataStat
{
    private FileSystem fileSystem = null;
    private List<FileStatus> fileStatuses = new ArrayList<>();
    private List<String> fieldNames = new ArrayList<>();
    private int columnCount = 0;
    private int rowGroupCount = 0;
    private long rowCount = 0;

    List<long[]> columnChunkSizes = new ArrayList<>();

    public PixelsMetadataStat (String nameNode, int hdfsPort, String dirPath) throws IOException, MetadataException
    {
        Configuration conf = new Configuration();
        // set readahead to 1 can avoid Premature EOF Error of HDFS.
        // although this is a debug level error, it is not elegant.
        //conf.set("dfs.datanode.readahead.bytes", "1");
        //conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        //conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        this.fileSystem = FileSystem.get(URI.create("hdfs://" + nameNode + ":" + hdfsPort), conf);
        Path hdfsDirPath = new Path(dirPath);
        if (fileSystem.isDirectory(hdfsDirPath))
        {
            FileStatus[] fileStatuses = fileSystem.listStatus(hdfsDirPath);
            for (FileStatus status : fileStatuses)
            {
                if (status.isFile())
                {
                    PixelsReader reader = PixelsReaderImpl.newBuilder()
                            .setFS(this.fileSystem)
                            .setPath(status.getPath()).build();
                    if (this.fieldNames.isEmpty())
                    {
                        // init fieldNames and cholumnChunkSizeSums
                        this.fieldNames.addAll(reader.getFileSchema().getFieldNames());
                        this.columnCount = this.fieldNames.size();
                    }
                    this.fileStatuses.add(status);
                    this.rowCount += reader.getNumberOfRows();
                    this.rowGroupCount += reader.getRowGroupNum();
                    for (int i = 0; i < reader.getRowGroupNum(); ++i)
                    {
                        long[] sizes = new long[this.columnCount];
                        for (int j = 0; j < this.columnCount; ++j)
                        {
                            sizes[j] = reader.getRowGroupFooter(i).
                                    getRowGroupIndexEntry().getColumnChunkIndexEntries(j).getChunkLength();
                        }
                        this.columnChunkSizes.add(sizes);
                    }
                    reader.close();
                }
            }
        }

        if (this.fileStatuses.isEmpty())
        {
            throw new MetadataException(dirPath + " is empty or not a directory.");
        }
    }

    /**
     * get the number of rows
     *
     * @return
     */
    @Override
    public long getRowCount()
    {
        return this.rowCount;
    }

    /**
     * get the average column chunk size of all the row groups
     *
     * @return
     */
    @Override
    public double[] getAvgColumnChunkSize()
    {
        double[] avgs = new double[columnCount];
        for (int i = 0; i < this.columnCount; ++i)
        {
            avgs[i] = 0;
        }
        for (long[] sizes : this.columnChunkSizes)
        {
            for (int i = 0; i < this.columnCount; ++i)
            {
                avgs[i] += sizes[i];
            }
        }
        for (int i = 0; i < this.columnCount; ++i)
        {
            avgs[i] /= this.rowGroupCount;
        }

        return avgs;
    }

    /**
     * get the standard deviation of the column chunk sizes.
     *
     * @param avgSize
     * @return
     */
    @Override
    public double[] getColumnChunkSizeStdDev(double[] avgSize) throws MetadataException
    {
        double[] dev = new double[this.columnCount];

        for (int i = 0; i < this.columnCount; ++i)
        {
            dev[i] = 0;
        }

        for (long[] sizes : this.columnChunkSizes)
        {
            for (int i = 0; i < this.columnCount; ++i)
            {
                dev[i] += Math.pow(sizes[i] - avgSize[i], 2);
            }
        }

        for (int i = 0; i < this.columnCount; ++i)
        {
            dev[i] = Math.sqrt(dev[i] / this.rowGroupCount);
        }

        return dev;
    }

    /**
     * get the field (column) names.
     *
     * @return
     */
    @Override
    public List<String> getFieldNames()
    {
        return new ArrayList<>(this.fieldNames);
    }

    /**
     * get the number of files.
     *
     * @return
     */
    @Override
    public int getFileCount()
    {
        return this.fileStatuses.size();
    }

    /**
     * get the number of row groups.
     *
     * @return
     */
    @Override
    public int getRowGroupCount()
    {
        return this.rowGroupCount;
    }

    /**
     * get the average compressed size of the rows in the files.
     *
     * @return
     */
    @Override
    public double getRowSize()
    {
        return (double) this.getTotalSize() / this.rowCount;
    }

    /**
     * get the total compressed size of the files.
     *
     * @return
     */
    @Override
    public long getTotalSize()
    {
        long totalSize = 0;
        for (long[] sizes : this.columnChunkSizes)
        {
            for (int i = 0; i < this.columnCount; ++i)
            {
                totalSize += sizes[i];
            }
        }
        return totalSize;
    }
}
