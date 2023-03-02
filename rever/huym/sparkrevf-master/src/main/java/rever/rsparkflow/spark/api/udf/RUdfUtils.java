package rever.rsparkflow.spark.api.udf;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;

/**
 * @author anhlt (andy)
 * @since 19/05/2022
 **/

public class RUdfUtils implements Serializable {

    private static final long serialVersionUID = 5473300292979135115L;

    /**
     * UDF to get domain from a URL
     */
    public static final String RV_GET_DOMAIN = "rvGetDomain";

    /**
     * UDF to parse client info from user agent.
     */
    public static final String RV_PARSE_USER_AGENT = "rvParseUserAgent";

    public static final String RV_PARSE_SALE_PIPELINE_CUSTOMER = "rvParseSalePipelineCustomer";


    /**
     * This udf will apply the following logic to the given column
     * column match{
     * case null => defaultValue
     * case _ => column
     * }
     */
    public static final String RV_COALESCE = "rvCoalesce";

    public static final String RV_DATE_TO_MILLIS = "rvDateToMillis";
    public static final String RV_DETECT_BUSINESS_UNIT = "rvDetectBusinessUnit";

    private RUdfUtils() {

    }

    /**
     * Register all common UDF from REVER implementations
     *
     * @param sparkSession
     */
    public static void registerAll(SparkSession sparkSession) {
        sparkSession.udf().register(RV_COALESCE, new CoalesceUdf(), DataTypes.StringType);
        sparkSession.udf().register(RV_GET_DOMAIN, new DomainUdf(), DataTypes.StringType);
        sparkSession.udf().register(RV_DATE_TO_MILLIS, new DateToMillisUdf(), DataTypes.LongType);
        sparkSession.udf().register(RV_PARSE_USER_AGENT, new UserAgentUdf(), UserAgentUdf.outputDataType());
        sparkSession.udf().register(RV_PARSE_SALE_PIPELINE_CUSTOMER, new SalePipelineCustomerUdf(), SalePipelineCustomerUdf.outputDataType());
        sparkSession.udf().register(RV_DETECT_BUSINESS_UNIT, new DetectBusinessUnitUdf(), DataTypes.StringType);
    }
}


