package silverLayer.netsuiteIntegration.model

import com.amazonaws.services.glue.log.GlueLogger
import org.apache.spark.sql.DataFrame

trait AllRegionsTable {
    def unionAllRegionsData(mxDf: DataFrame, brDf: DataFrame, coDf: DataFrame, logger: GlueLogger): DataFrame
}
