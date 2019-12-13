package cn.edu.ruc.iir.rainbow.seek;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.rainbow.seek
 * @ClassName: Constants
 * @Description: constant parameters
 * @author: tao
 * @date: Create in 2019-12-13 19:21
 **/
public class Constants {
    public static final String basePath = "/home/tao/software/station/bitbucket/rainbow/";
    public static final String workloadPath = basePath + "rainbow-server/src/main/resources/pipeline/2a37a292b82a7227da22fc6c28e508bd/workload.txt";
    public static final String estimateTimePath = basePath + "rainbow-server/src/main/resources/pipeline/2a37a292b82a7227da22fc6c28e508bd/estimate_duration.csv";
    public static final String estimateTimePath_Ordered = basePath + "rainbow-server/src/main/resources/pipeline/2a37a292b82a7227da22fc6c28e508bd/estimate_duration_ordered.csv";
    public static final String workloadTunePath = basePath + "rainbow-server/src/main/resources/rl/workload.txt";
    public static final String workloadCostPath = basePath + "rainbow-server/src/main/resources/rl/cost.txt";

    public static final String ddlPath = basePath + "rainbow-server/src/main/resources/pipeline/2a37a292b82a7227da22fc6c28e508bd/parquet_ddl.sql";
    public static final String ddlPath_Ordered = basePath + "rainbow-server/src/main/resources/pipeline/2a37a292b82a7227da22fc6c28e508bd/parquet_ordered_ddl.sql";

    public static final String columnPath_Tune = basePath + "rainbow-server/src/main/resources/rl/column.txt";
    public static final String columnPath_Ordered_Tune = basePath + "rainbow-server/src/main/resources/rl/column_ordered.txt";
    public static final String schemaPath = basePath + "rainbow-server/src/main/resources/rl/schema.txt";
    public static final String estimate_Duration_Path = basePath + "rainbow-server/src/main/resources/rl/estimate_duration.csv";

    public static final String schemaPath_Ordered = basePath + "rainbow-server/src/main/resources/rl/schema_ordered.txt";

}
