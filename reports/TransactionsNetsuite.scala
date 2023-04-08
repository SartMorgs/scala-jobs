package goldLayer.netsuiteReports

import com.amazonaws.services.glue.GlueContext
import org.apache.spark.sql.DataFrame
import com.amazonaws.services.glue.log.GlueLogger
import org.apache.spark.sql.functions.{to_timestamp,col}
import utils.delta.DeltaTableUtils

class TransactionsNetsuite extends NetsuiteDataReport {
    val INSERT_DATE_COLUMN = "insert_date"
    val UUID_COLUMN = "uuid"
    val DATE_COLUMN = "date"
    val ACCOUNTING_DATE_COLUMN = "accounting_date"
    val INVOICE_COLUMN = "invoice"
    val INVOICE_DESCRIPTION_COLUMN = "invoice_description"
    val TYPE_COLUMN = "type"
    val ID_CLIENT_COLUMN = "id_client"
    val AMOUNT_DEBIT_COLUMN = "amount_d"
    val AMOUNT_CREDIT_COLUMN = "amount_c"
    val NORMAL_CORRECTORA_COLUMN = "normal_correctora"
    val ACCOUNT_ID_COLUMN = "account_id"

    def getNew(newData: DataFrame, oldData: DataFrame, logger: GlueLogger): DataFrame = {
        if(oldData.isEmpty) {
            newData
        } else {
            val netsuiteDataHelper = new NetsuiteDataHelper
            val maxDateNumberColumn = "max_date"
            val lastDateOldDf = oldData
                .select(INSERT_DATE_COLUMN)
                .withColumn(INSERT_DATE_COLUMN, to_timestamp(col(INSERT_DATE_COLUMN), netsuiteDataHelper.DATETIME_FORMAT))
                .sort(col(INSERT_DATE_COLUMN).desc)
                .withColumnRenamed(INSERT_DATE_COLUMN, s"old_$INSERT_DATE_COLUMN")
                .first()(0)
            logger.info("Got new transactions comparing silver and gold layer tables!")
            newData
                .withColumn(INSERT_DATE_COLUMN, to_timestamp(col(INSERT_DATE_COLUMN), netsuiteDataHelper.DATETIME_FORMAT))
                .filter(col(INSERT_DATE_COLUMN) > lastDateOldDf)
                .selectExpr(INSERT_DATE_COLUMN, UUID_COLUMN, DATE_COLUMN, ACCOUNTING_DATE_COLUMN, INVOICE_COLUMN, INVOICE_DESCRIPTION_COLUMN,
                    TYPE_COLUMN, ID_CLIENT_COLUMN, AMOUNT_DEBIT_COLUMN, AMOUNT_CREDIT_COLUMN, NORMAL_CORRECTORA_COLUMN,
                    ACCOUNT_ID_COLUMN, netsuiteDataHelper.YEAR_COLUMN, netsuiteDataHelper.MONTH_COLUMN,
                    netsuiteDataHelper.DAY_COLUMN)
        }
    }

    def saveNewOnGoldLayer(
        silverLayerDataPath: String, goldLayerDataPath: String, glueContext: GlueContext, logger: GlueLogger
    ): Unit = {
        val deltaTableUtils = new DeltaTableUtils(glueContext.getSparkSession)
        val silverLayerDataDf = deltaTableUtils.readDeltaTable(silverLayerDataPath).toDF
        val goldLayerDataDf = deltaTableUtils.returnDf(goldLayerDataPath)
        val newTransactionsReportDf = getNew(silverLayerDataDf, goldLayerDataDf, logger)

        val primaryKey = s"$UUID_COLUMN,$ACCOUNT_ID_COLUMN"
        deltaTableUtils.performUpsert(goldLayerDataPath, newTransactionsReportDf, primaryKey, true)
    }
}
