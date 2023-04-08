package goldLayer.netsuiteReports

import com.amazonaws.services.glue.GlueContext
import org.apache.spark.sql.DataFrame
import com.amazonaws.services.glue.log.GlueLogger
import org.apache.spark.sql.functions.{col,lit,substring}
import utils.delta.DeltaTableUtils

class InvoicesNotMappedNetsuite {
    val INVOICE_COLUMN = "invoice"
    val TYPE_COLUMN = "type"
    val COUNTRY_COLUMN = "country"
    val ID_CLIENT_COLUMN = "id_client"
    val AMOUNT_DEBIT_COLUMN = "amount_d"
    val AMOUNT_CREDIT_COLUMN = "amount_c"
    val NORMAL_CORRECTORA_COLUMN = "normal_correctora"

    val netsuiteDataHelper = new NetsuiteDataHelper

    def getCurrentData(sourceDf: DataFrame, logger: GlueLogger): DataFrame = {
        val colsToSelect = Seq(
            INVOICE_COLUMN,
            TYPE_COLUMN,
            COUNTRY_COLUMN,
            NORMAL_CORRECTORA_COLUMN
        )
        val normalInvoices = sourceDf
            .filter(col(AMOUNT_DEBIT_COLUMN) >= 0 || col(AMOUNT_CREDIT_COLUMN) >= 0)
            .withColumn(NORMAL_CORRECTORA_COLUMN, lit("Normal"))
        val correctoraInvoices = sourceDf
            .filter(col(AMOUNT_DEBIT_COLUMN) < 0 || col(AMOUNT_CREDIT_COLUMN) < 0)
            .withColumn(NORMAL_CORRECTORA_COLUMN, lit("Correctora"))
        val distinctInvoicesDf = normalInvoices.union(correctoraInvoices)
            .withColumn(COUNTRY_COLUMN, substring(col(ID_CLIENT_COLUMN), 0, 2))
            .selectExpr(colsToSelect: _*)
            .distinct
        netsuiteDataHelper.getDataWithCurrentDataAsYearMonthDay(distinctInvoicesDf, logger)
    }

    def saveCurrentDataOnGoldLayer(
        invoicesDf: DataFrame, goldLayerDataPath: String, glueContext: GlueContext, logger: GlueLogger
    ): Unit = {
        val deltaTableUtils = new DeltaTableUtils(glueContext.getSparkSession)
        val primaryKey = s"$INVOICE_COLUMN,$TYPE_COLUMN,$NORMAL_CORRECTORA_COLUMN,$COUNTRY_COLUMN"
        deltaTableUtils.performUpsert(goldLayerDataPath, invoicesDf, primaryKey, true)
    }

    def dataFilteredByCountry(invoicesDf: DataFrame, country: String): DataFrame = {
        invoicesDf.filter(col(COUNTRY_COLUMN) === country)
    }
}
