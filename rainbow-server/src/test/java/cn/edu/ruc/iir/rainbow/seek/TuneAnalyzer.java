package cn.edu.ruc.iir.rainbow.seek;

import cn.edu.ruc.iir.rainbow.common.FileUtils;
import org.junit.Test;

import java.io.IOException;

import static cn.edu.ruc.iir.rainbow.seek.Constants.*;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.seek
 * @ClassName: TuneAnalyzer
 * @Description:
 * @author: tao
 * @date: Create in 2019-12-13 19:11
 **/
public class TuneAnalyzer {

    @Test
    public void getColumn() throws IOException {
        String columns = AnalyzerUtil.getLayoutFromSQL(ddlPath);
        System.out.println(columns);
        FileUtils.writeFile(columns, columnPath_Tune);

        String columns_ordered = AnalyzerUtil.getLayoutFromSQL(ddlPath_Ordered);
        System.out.println(columns_ordered);
        FileUtils.writeFile(columns_ordered, columnPath_Ordered_Tune);
    }

}
