package cn.edu.ruc.iir.rainbow.common.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class FileUtils
{
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

    private void write(String aCashe) throws IOException
    {
        File file = new File(this.getClass().getClassLoader()
                .getResource(("cashe/cashe.txt")).getFile());
        String filename = file.getAbsolutePath();
        filename = filename.replace("cashe.txt", "20170518205458.txt");
        // filename = filename.replace("cashe.txt", DateUtil.mkTime(new Date())
        // + ".txt");
        System.out.println(filename);
        FileUtils.appendFile(aCashe, filename);
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
}
