package goldLayer.netsuiteReports

import org.apache.spark.sql.DataFrame
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.GlueContext

trait NetsuiteDataReport {
    def getNew(newData: DataFrame, oldData: DataFrame, logger: GlueLogger): DataFrame
    def saveNewOnGoldLayer(
        silverLayerDataPath: String, goldLayerDataPath: String, glueContext: GlueContext, logger: GlueLogger): Unit
}
