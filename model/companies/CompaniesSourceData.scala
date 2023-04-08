package silverLayer.netsuiteIntegration.companies

import com.amazonaws.services.glue.log.GlueLogger
import org.apache.spark.sql.DataFrame

trait CompaniesSourceData {
    def selectColumns(sourceDf: DataFrame, country: String, logger: GlueLogger): DataFrame
    def getTable(sourcePath: String, country: String, logger: GlueLogger): DataFrame
}
