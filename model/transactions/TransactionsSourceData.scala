package silverLayer.netsuiteIntegration.transactions

import com.amazonaws.services.glue.log.GlueLogger
import org.apache.spark.sql.DataFrame

trait TransactionsSourceData {
    def selectColumns(sourceDf: DataFrame, country: String, logger: GlueLogger): DataFrame
    def getTable(sourcePath: String, country: String, logger: GlueLogger): DataFrame
    def getAllRegions(mxSourcePath: String, brSourcePath: String, coSourcePath: String, logger: GlueLogger): DataFrame
}
