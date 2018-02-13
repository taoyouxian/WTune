package cn.edu.ruc.iir.rainbow.common;

/**
 * @ClassName: Settings
 * @Title:
 * @Description: Common variables for the system
 * @param:
 * @author: Tao
 * @date: 11:05 2017/7/31
 */
public class Settings
{
    // the upperbound of the random number
    public static final int DATA_MAX = 40000;
    // the unit of the _data generated
    public static final long MB = 1 * 1024 * 1024L;
    public static final int BUFFER_SIZE = 1024 * 1024 * 32;
    public static String TEMPLATE_DIRECTORY = null;

    public static final String WORKLOAD_POST_URL = "http://127.0.0.1:8080/rw/clientUpload";

    public static final String WORKLOAD_PATH = "/home/tao/software/station/Workplace/workload.txt";

    public static boolean APC_TASK = false;
    public static final String APC_PATH = "/home/tao/software/station/Workplace/APC.txt";

}
