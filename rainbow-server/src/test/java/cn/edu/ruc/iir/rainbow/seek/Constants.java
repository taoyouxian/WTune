package cn.edu.ruc.iir.rainbow.seek;

import cn.edu.ruc.iir.rainbow.common.ConfigFactory;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.seek
 * @ClassName: Constants
 * @Description: constant parameters
 * @author: tao
 * @date: Create in 2019-12-13 19:21
 **/
public class Constants {
    public static final String basePath = ConfigFactory.Instance().getProperty("tune.path");
    // query analyzer
    public static final String workloadPath = basePath + "pipeline/2a37a292b82a7227da22fc6c28e508bd/workload.txt";
    public static final String estimateTimePath = basePath + "pipeline/2a37a292b82a7227da22fc6c28e508bd/estimate_duration.csv";
    public static final String estimateTimePath_Ordered = basePath + "pipeline/2a37a292b82a7227da22fc6c28e508bd/estimate_duration_ordered.csv";

    public static final String default_SchemaPath = basePath + "rl/schema.txt";
    public static final String default_WorkloadPath = basePath + "rl/workload.txt";
    public static final String workloadCostPath = basePath + "rl/cost.txt";

    // tune analyzer
    public static final String ddlPath = basePath + "pipeline/2a37a292b82a7227da22fc6c28e508bd/parquet_ddl.sql";
    public static final String ddlPath_Ordered = basePath + "pipeline/2a37a292b82a7227da22fc6c28e508bd/parquet_ordered_ddl.sql";
    public static final String columnPath_Tune = basePath + "rl/column.txt";
    public static final String columnPath_Ordered_Tune = basePath + "rl/column_ordered.txt";

    // cost analyzer, scatter analyzer
    public static final String estimate_Duration_Path = basePath + "rl/estimate_duration.csv";
    public static final String estimated_Duration_Path_Ordered = basePath + "rl/estimate_duration_ordered.csv";
    public static final String joined_Estimated_Duration_Path = basePath + "rl/estimate_duration_joined_id.csv";
    // schema analyzer
    public static final String schemaPath_Ordered = basePath + "rl/schema_ordered.txt";
    // layout analyzer
    public static final String layout_Ordered = basePath + "layout/schema_ordering.txt";

}
