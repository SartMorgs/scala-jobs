package goldLayer.netsuiteReports

import com.amazonaws.services.glue.GlueContext
import org.apache.spark.sql.DataFrame
import com.amazonaws.services.glue.log.GlueLogger
import org.apache.spark.sql.functions.{to_timestamp,col}
import utils.delta.DeltaTableUtils

class ClientsNetsuite extends NetsuiteDataReport {
    val CREATED_AT_COLUMN = "created_at"
    val COMPANY_NAME_COLUMN = "company_name"
    val TAX_ID_COLUMN = "tax_id"
    val ID_CLARA_CONTRACT_COLUMN = "id_clara_contract"
    val COUNTRY_COLUMN = "country"

    def getNew(newData: DataFrame, oldData: DataFrame, logger: GlueLogger): DataFrame = {
        if(oldData.isEmpty) {
            newData
        } else {
            val netsuiteDataHelper = new NetsuiteDataHelper
            val maxDateNumberColumn = "max_date"
            val lastDateOldDf = oldData
                .select(CREATED_AT_COLUMN)
                .withColumn(CREATED_AT_COLUMN, to_timestamp(col(CREATED_AT_COLUMN), netsuiteDataHelper.DATETIME_FORMAT))
                .sort(col(CREATED_AT_COLUMN).desc)
                .withColumnRenamed(CREATED_AT_COLUMN, s"old_$CREATED_AT_COLUMN")
                .first()(0)
            logger.info("Got new clients comparing silver and gold layer tables!")
            val colsToSelect = Seq(
                CREATED_AT_COLUMN,
                COMPANY_NAME_COLUMN,
                TAX_ID_COLUMN,
                ID_CLARA_CONTRACT_COLUMN,
                COUNTRY_COLUMN,
                netsuiteDataHelper.YEAR_COLUMN,
                netsuiteDataHelper.MONTH_COLUMN,
                netsuiteDataHelper.DAY_COLUMN
            )
            newData
                .withColumn(CREATED_AT_COLUMN, to_timestamp(col(CREATED_AT_COLUMN), netsuiteDataHelper.DATETIME_FORMAT))
                .filter(col(CREATED_AT_COLUMN) > lastDateOldDf)
                .selectExpr(colsToSelect: _*)
        }
    }

    def getUpdated(newData: DataFrame, oldData: DataFrame, logger: GlueLogger): DataFrame = {
        val colsToSelect = Seq(
            s"$ID_CLARA_CONTRACT_COLUMN as old_$ID_CLARA_CONTRACT_COLUMN",
            s"$COMPANY_NAME_COLUMN as old_$COMPANY_NAME_COLUMN",
            s"$TAX_ID_COLUMN as old_$TAX_ID_COLUMN"
        )
        val selectOldData = oldData
            .selectExpr(colsToSelect: _*)
        logger.info("Got updated clients comparing silver and gold layer tables!")
        newData
            .join(selectOldData)
            .filter(s"$ID_CLARA_CONTRACT_COLUMN == old_$ID_CLARA_CONTRACT_COLUMN")
            .filter(s"$COMPANY_NAME_COLUMN <> old_$COMPANY_NAME_COLUMN OR $TAX_ID_COLUMN <> old_$TAX_ID_COLUMN")
            .drop(s"old_$ID_CLARA_CONTRACT_COLUMN", s"old_$COMPANY_NAME_COLUMN", s"old_$TAX_ID_COLUMN")
    }

    def saveNewOnGoldLayer(
        silverLayerDataPath: String, goldLayerDataPath: String, glueContext: GlueContext, logger: GlueLogger
    ): Unit = {
        val deltaTableUtils = new DeltaTableUtils(glueContext.getSparkSession)
        val silverLayerDataDf = deltaTableUtils.readDeltaTable(silverLayerDataPath).toDF
        val goldLayerDataDf = deltaTableUtils.returnDf(goldLayerDataPath)
        val newActiveClientsReportDf = getNew(silverLayerDataDf, goldLayerDataDf, logger)

        deltaTableUtils.performUpsert(goldLayerDataPath, newActiveClientsReportDf, ID_CLARA_CONTRACT_COLUMN, true)
    }

    def saveUpdatedOnGoldLayer(
        silverLayerDataPath: String, goldLayerDataPath: String, glueContext: GlueContext, logger: GlueLogger
    ): Unit = {
        val deltaTableUtils = new DeltaTableUtils(glueContext.getSparkSession)
        val silverLayerDataDf = deltaTableUtils.readDeltaTable(silverLayerDataPath).toDF
        val goldLayerDataDf = deltaTableUtils.returnDf(goldLayerDataPath)
        val updatedClientsReportDf = getUpdated(silverLayerDataDf, goldLayerDataDf, logger)

        deltaTableUtils.performUpsert(goldLayerDataPath, updatedClientsReportDf, ID_CLARA_CONTRACT_COLUMN, true)
    }
}
