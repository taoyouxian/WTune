package cn.edu.ruc.iir.rainbow.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;

public class FileUtils
{
    static private FileUtils instance = null;

    private FileUtils()
    {
    }

    public static FileUtils Instance()
    {
        if (instance == null)
        {
            instance = new FileUtils();
        }
        return instance;
    }

    public BufferedReader getReader(String path) throws FileNotFoundException
    {
        BufferedReader reader = new BufferedReader(new FileReader(path));
        return reader;
    }

    public File[] getFiles(String dirPath)
    {
        File dir = new File(dirPath);
        return dir.listFiles();
    }

    public FileStatus[] getHDFSFileStatuses(String dirPath, Configuration conf) throws IOException
    {
        FileSystem fs = FileSystem.get(URI.create(dirPath), conf);
        return fs.listStatus(new Path(dirPath));
    }

    /**
     * path must in form of hdfs://namenode:port/path/
     * @param path
     * @return
     * @throws IOException
     */
    public boolean HDFSPathExists(String path) throws IOException
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(path), conf);
        return fs.exists(new Path(path));
    }

    /**
     * @param fileName
     * @return String
     * @Title: readFile
     * @Description:方法的重载
     */
    public static String readFileToString(String fileName)
    {
        try
        {
            return org.apache.commons.io.FileUtils.
                    readFileToString(new File(fileName));
        } catch (IOException e)
        {
            e.printStackTrace();
            return null;
        }
    }

    public static void writeFile(String content, String filename)
            throws IOException
    {
        // 要写入的文件
        File file = new File(filename);
        // 写入流对象
        PrintWriter printWriter = null;
        try
        {
            printWriter = new PrintWriter(file);
            printWriter.print(content);
            printWriter.flush();
        } catch (Exception e)
        {
            e.printStackTrace();
        } finally
        {
            if (printWriter != null)
            {
                try
                {
                    printWriter.close();
                } catch (Exception e2)
                {
                    e2.printStackTrace();
                }
            }
        }
    }


    public static void writeFile(String content, String filename, boolean flag)
            throws IOException
    {
        File file = new File(filename);
        FileWriter fw = new FileWriter(file, flag);
        // 写入流对象
        PrintWriter printWriter = null;
        try
        {
            printWriter = new PrintWriter(fw);
            printWriter.print(content);
            printWriter.flush();
        } catch (Exception e)
        {
            e.printStackTrace();
        } finally
        {
            if (printWriter != null)
            {
                try
                {
                    printWriter.close();
                } catch (Exception e2)
                {
                    e2.printStackTrace();
                }
            }
        }
    }

    public static void appendFile(String content, String filename)
            throws IOException
    {
        boolean flag = false;
        // 要写入的文件
        File file = new File(filename);
        if (file.exists())
        {
            flag = true;
        }
        FileWriter fw = new FileWriter(file, true);
        // 写入流对象
        PrintWriter printWriter = null;
        try
        {
            printWriter = new PrintWriter(fw);
            if (flag)
            {
                printWriter.print("\r\n");
            }
            printWriter.print(content);
            printWriter.flush();
        } catch (Exception e)
        {
            e.printStackTrace();
        } finally
        {
            if (printWriter != null)
            {
                try
                {
                    printWriter.close();
                } catch (Exception e2)
                {
                    e2.printStackTrace();
                }
            }
        }
    }

    public static void deleteDirectory (String fileName)
    {
        try
        {
            org.apache.commons.io.FileUtils.deleteDirectory(new File(fileName));
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public BufferedWriter getWriter(String path) throws IOException
    {
        return new BufferedWriter(new FileWriter(path));
    }

    public static String readFile(String filename) {
        StringBuilder sb = new StringBuilder();
        try (FileReader reader = new FileReader(filename);
             BufferedReader br = new BufferedReader(reader)
        )
        {
            String line;
            while ((line = br.readLine()) != null)
            {
                sb.append(line);
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static void deleteFile(String filename)
    {
        File file = new File(filename);
        if (file.isFile())
            file.delete();
    }

    public static void mkdir(String dirname)
    {
        File dir = new File(dirname);
        if (!dir.exists())
            dir.mkdirs();
    }

    public static void deleteDir(String filename)
    {
        File file = new File(filename);
        if (file.isFile())
            file.delete();
        else
        {
            File[] files = file.listFiles();
            if(files == null){
                file.delete();
            }
            else
            {
                for (int i = 0; i < files.length; i++)
                {
                    deleteDir(files[i].getAbsolutePath());
                }
                file.delete();
            }
        }
    }
}
