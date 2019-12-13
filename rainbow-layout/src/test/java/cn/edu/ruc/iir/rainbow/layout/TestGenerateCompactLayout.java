package cn.edu.ruc.iir.rainbow.layout;

import cn.edu.ruc.iir.pixels.common.metadata.domain.Compact;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.LayoutDao;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

/**
 * Created at: 18-12-31
 * Author: hank
 */
public class TestGenerateCompactLayout
{
    @Test
    public void testGenNaive ()
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        configFactory.addProperty("metadata.db.url", "jdbc:mysql://dbiir27:3306/pixels_metadata?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull");
        configFactory.addProperty("metadata.db.password", "pixels27");
        LayoutDao layoutDao = new LayoutDao();
        Layout layout = layoutDao.getById(3);
        String compactStr = layout.getCompact();
        Compact compact = JSONObject.parseObject(compactStr, Compact.class);
        compact.setNumRowGroupInBlock(32);
        compact.getColumnletOrder().clear();
        for (int i = 0; i < 32; ++i)
        {
            for (int j = 0; j < 105; ++j)
            {
                compact.addColumnletOrder(i + ":" + j);
            }
        }

        layout.setCompact(JSON.toJSONString(compact));
        layoutDao.update(layout);
    }

    @Test
    public void testGenOpt ()
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        configFactory.addProperty("metadata.db.url", "jdbc:mysql://dbiir27:3306/pixels_metadata?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull");
        configFactory.addProperty("metadata.db.password", "pixels27");
        LayoutDao layoutDao = new LayoutDao();
        Layout layout = layoutDao.getById(4);
        String compactStr = layout.getCompact();
        Compact compact = JSONObject.parseObject(compactStr, Compact.class);
        compact.setNumRowGroupInBlock(32);
        compact.getColumnletOrder().clear();
        for (int i = 0; i < 105; ++i)
        {
            for (int j = 0; j < 32; ++j)
            {
                compact.addColumnletOrder(j + ":" + i);
            }
        }
        layout.setCompact(JSON.toJSONString(compact));
        layoutDao.update(layout);
    }

    @Test
    public void readLayout()
    {
        ConfigFactory configFactory = ConfigFactory.Instance();
        configFactory.addProperty("metadata.db.url", "jdbc:mysql://dbiir27:3306/pixels_metadata?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull");
        configFactory.addProperty("metadata.db.password", "pixels27");
        LayoutDao layoutDao = new LayoutDao();
        Layout layout = layoutDao.getById(10);
        String compactStr = layout.getCompact();
        System.out.println(compactStr);
    }
}
