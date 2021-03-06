package cn.edu.ruc.iir.rainbow.common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigFactory
{
    private static ConfigFactory instance = null;

    private ConfigFactory()
    {
        prop = new Properties();
        String rainbowHome = System.getenv("RAINBOW_HOME");
        InputStream in = null;
        if (rainbowHome == null)
        {
            in = this.getClass().getResourceAsStream("/rainbow.properties");
        } else
        {
            if (!(rainbowHome.endsWith("/") || rainbowHome.endsWith("\\")))
            {
                rainbowHome += "/";
            }
            prop.setProperty("rainbow.home", rainbowHome);
            try
            {
                in = new FileInputStream(rainbowHome + "rainbow.properties");
            } catch (FileNotFoundException e)
            {
                e.printStackTrace();
            }
        }
        try
        {
            if (in != null)
            {
                prop.load(in);
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public static ConfigFactory Instance()
    {
        if (instance == null)
        {
            instance = new ConfigFactory();
        }
        return instance;
    }

    private Properties prop = null;

    public void loadProperties(String propFilePath)
    {
        FileInputStream in = null;
        try
        {
            in = new FileInputStream(propFilePath);
            this.prop.load(in);
        } catch (IOException e)
        {
            e.printStackTrace();
        } finally
        {
            if (in != null)
            {
                try
                {
                    in.close();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    public void addProperty(String key, String value)
    {
        this.prop.setProperty(key, value);
    }

    public String getProperty(String key)
    {
        return this.prop.getProperty(key);
    }
}
