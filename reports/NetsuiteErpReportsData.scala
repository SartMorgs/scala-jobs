package goldLayer.netsuiteReports

import com.amazonaws.services.glue.GlueContext
import org.apache.spark.{SparkConf, SparkContext}
import com.amazonaws.services.glue.log.GlueLogger
import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_date, date_add, dayofmonth, lit, max, month, to_timestamp, year}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.expressions.Window
import utils.delta.DeltaTableUtils
import utils.reports.ReportUtils

import java.util.Calendar
import java.text.SimpleDateFormat;

object NetsuiteErpReportsData {
    private val DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"
    private val START_DATE = "2023-01-01 00:00:00.000"

    private val SILVER_LAYER_DATA_PATH_ARG = "SILVER_LAYER_DATA_PATH"
    private val GOLD_LAYER_DATA_PATH_ARG = "GOLD_LAYER_DATA_PATH"
    private val REPORT_DATA_PATH_ARG = "REPORT_DATA_PATH"

    private val ACTIVE_CLIENTS_TABLE = "active_clients"
    private val TRANSACTIONS_TABLE = "transactions"
    private val TRANSACTIONS_OUT_OF_REPORT_TABLE = "transactions_out_of_report"
    private val INVOICES_NOT_MAPPED_TABLE = "invoices_not_mapped"

    def initializingSpark(logger: GlueLogger): GlueContext = {
        logger.info("Initializing Spark and Glue Context")

        val correctedString = "CORRECTED"
        val conf = new SparkConf()
        .setAppName("NetsuiteErpReportsData")
        .set("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .set("spark.sql.legacy.parquet.int96RebaseModeInRead", correctedString)
        .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", correctedString)
        .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", correctedString)
        .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", correctedString)

        val sc: SparkContext = new SparkContext(conf)
        sc.setLogLevel("INFO")

        val glueContext: GlueContext = new GlueContext(sc)

        glueContext
    }

    def generateReport(dataframe: DataFrame, reportPath: String, glueContext: GlueContext, logger: GlueLogger): Unit = {
        if(dataframe.isEmpty) {
            logger.info("There are no data for current date!")
        } else {
            ReportUtils.generateCsvReport(glueContext, dataframe, reportPath)
        }
    }

    def generateReportForEachCountry(dataframe: DataFrame, reportPath: String, country: String, glueContext: GlueContext, logger: GlueLogger): Unit = {
        val mxDf = dataframe.filter(col(country) === "MX")
        val mxReportPath = s"$reportPath/mx"
        generateReport(mxDf, mxReportPath, glueContext, logger)

        val brDf = dataframe.filter(col(country) === "BR")
        val brReportPath = s"$reportPath/br"
        generateReport(brDf, brReportPath, glueContext, logger)

        val coDf = dataframe.filter(col(country) === "CO")
        val coReportPath = s"$reportPath/co"
        generateReport(coDf, coReportPath, glueContext, logger)
    }

    def incomingClients(args: Map[String, String]): Unit = {
        val logger = new GlueLogger
        val glueContext = initializingSpark(logger)
        val netsuiteDataHelper = new NetsuiteDataHelper
        val clientsNetsuite = new ClientsNetsuite

        val tableKey = s"erp_reports/netsuite/$ACTIVE_CLIENTS_TABLE"

        val goldLayerDataPath = s"${args(GOLD_LAYER_DATA_PATH_ARG)}/$tableKey"
        val silverLayerDataPath = s"${args(SILVER_LAYER_DATA_PATH_ARG)}/$tableKey"
        val reportDataPath = s"${args(REPORT_DATA_PATH_ARG)}/incoming_clients"

        clientsNetsuite.saveNewOnGoldLayer(silverLayerDataPath, goldLayerDataPath, glueContext, logger)
        val goldLayerDataDf = new DeltaTableUtils(glueContext.getSparkSession).returnDf(goldLayerDataPath)

        val yesterdayDateClientsDf = netsuiteDataHelper.getDataForYesterday(
            goldLayerDataDf, clientsNetsuite.CREATED_AT_COLUMN)
        generateReport(yesterdayDateClientsDf, reportDataPath, glueContext, logger)
    }

    def updatedClients(args: Map[String, String]): Unit = {
        val logger = new GlueLogger
        val glueContext = initializingSpark(logger)
        val netsuiteDataHelper = new NetsuiteDataHelper
        val clientsNetsuite = new ClientsNetsuite

        val tableKey = s"erp_reports/netsuite/$ACTIVE_CLIENTS_TABLE"

        val silverLayerDataPath = s"${args(SILVER_LAYER_DATA_PATH_ARG)}/$tableKey"
        val goldLayerDataPath = s"${args(GOLD_LAYER_DATA_PATH_ARG)}/$tableKey"
        val reportDataPath = s"${args(REPORT_DATA_PATH_ARG)}/updated_clients"

        val silverLayerDataDf = DeltaTable.forPath(silverLayerDataPath).toDF
        val goldLayerDataDf = new DeltaTableUtils(glueContext.getSparkSession).returnDf(goldLayerDataPath)

        val updatedClients = clientsNetsuite.getUpdated(silverLayerDataDf, goldLayerDataDf, logger)
            .drop(netsuiteDataHelper.YEAR_COLUMN, netsuiteDataHelper.MONTH_COLUMN, netsuiteDataHelper.DAY_COLUMN)
        generateReport(updatedClients, reportDataPath, glueContext, logger)

        clientsNetsuite.saveUpdatedOnGoldLayer(silverLayerDataPath, goldLayerDataPath, glueContext, logger)
    }

    def incomingTransactions(args: Map[String, String]): Unit = {
        val logger = new GlueLogger
        val glueContext = initializingSpark(logger)
        val netsuiteDataHelper = new NetsuiteDataHelper
        val transactionsNetsuite = new TransactionsNetsuite

        val tableKey = s"erp_reports/netsuite/$TRANSACTIONS_TABLE"

        val goldLayerDataPath = s"${args(GOLD_LAYER_DATA_PATH_ARG)}/$tableKey"
        val reportDataPath = s"${args(REPORT_DATA_PATH_ARG)}/incoming_transactions"
        val silverLayerDataPath = s"${args(SILVER_LAYER_DATA_PATH_ARG)}/$tableKey"

        transactionsNetsuite.saveNewOnGoldLayer(silverLayerDataPath, goldLayerDataPath, glueContext, logger)
        val goldLayerDataDf = new DeltaTableUtils(glueContext.getSparkSession).returnDf(goldLayerDataPath)

        val yesterdayDateTransactionsDf = netsuiteDataHelper.getDataForYesterday(
            goldLayerDataDf, transactionsNetsuite.INSERT_DATE_COLUMN)
        generateReport(yesterdayDateTransactionsDf, reportDataPath, glueContext, logger)
    }

    def getHistoricalData(sourceDf: DataFrame, dateField: String, logger: GlueLogger): DataFrame = {
        logger.info("Got historical active clients!")
        sourceDf
            .withColumn(dateField, to_timestamp(col(dateField), DATETIME_FORMAT))
            .filter(col(dateField) >= START_DATE)
    }

    def invoicesNotMapped(args: Map[String, String]): Unit = {
        val logger = new GlueLogger
        var glueContext = initializingSpark(logger)
        val invoicesNotMappedNetsuite = new InvoicesNotMappedNetsuite

        val transactionsOutOfReportKey = s"erp_reports/netsuite/$TRANSACTIONS_OUT_OF_REPORT_TABLE"
        val invoicesNotMappedKey = s"erp_reports/netsuite/$INVOICES_NOT_MAPPED_TABLE"

        val silverLayerDataPath = s"${args(SILVER_LAYER_DATA_PATH_ARG)}/$transactionsOutOfReportKey"
        val goldLayerDataPath = s"${args(GOLD_LAYER_DATA_PATH_ARG)}/$invoicesNotMappedKey"
        val reportDataPath = s"${args(REPORT_DATA_PATH_ARG)}/invoices_not_mapped"

        val silverLayerDataDf = DeltaTable.forPath(silverLayerDataPath).toDF
        val invoicesNotMappedDf = invoicesNotMappedNetsuite.getCurrentData(silverLayerDataDf, logger)
        invoicesNotMappedNetsuite.saveCurrentDataOnGoldLayer(invoicesNotMappedDf, goldLayerDataPath, glueContext, logger)

        generateReportForEachCountry(
            invoicesNotMappedDf,
            reportDataPath,
            invoicesNotMappedNetsuite.COUNTRY_COLUMN,
            glueContext,
            logger
        )
    }

    def historialActiveClients(args: Map[String, String]): Unit = {
        val logger = new GlueLogger
        var glueContext = initializingSpark(logger)
        val clientsNetsuite = new ClientsNetsuite

        val tableKey = s"erp_reports/netsuite/$ACTIVE_CLIENTS_TABLE"

        val silverLayerDataPath = s"${args(SILVER_LAYER_DATA_PATH_ARG)}/$tableKey"
        val reportDataPath = s"${args(REPORT_DATA_PATH_ARG)}/incoming_clients"
        val goldLayerDataPath = s"${args(GOLD_LAYER_DATA_PATH_ARG)}/$tableKey"

        val silverLayerDataDf = DeltaTable.forPath(silverLayerDataPath).toDF
        clientsNetsuite.saveUpdatedOnGoldLayer(silverLayerDataPath, goldLayerDataPath, glueContext, logger)

        val historicalActiveClientsDf = getHistoricalData(silverLayerDataDf, clientsNetsuite.CREATED_AT_COLUMN, logger)
        generateReport(historicalActiveClientsDf, reportDataPath, glueContext, logger)
    }

    def historialTransactions(args: Map[String, String]): Unit = {
        val logger = new GlueLogger
        var glueContext = initializingSpark(logger)
        val transactionsNetsuite = new TransactionsNetsuite

        val tableKey = s"erp_reports/netsuite/$TRANSACTIONS_TABLE"

        val silverLayerDataPath = s"${args(SILVER_LAYER_DATA_PATH_ARG)}/$tableKey"
        val goldLayerDataPath = s"${args(GOLD_LAYER_DATA_PATH_ARG)}/$tableKey"
        val reportDataPath = s"${args(REPORT_DATA_PATH_ARG)}/incoming_transactions"

        val silverLayerDataDf = DeltaTable.forPath(silverLayerDataPath).toDF
        transactionsNetsuite.saveNewOnGoldLayer(silverLayerDataPath, goldLayerDataPath, glueContext, logger)

        val historicalTransactionsDf = getHistoricalData(
            silverLayerDataDf, transactionsNetsuite.INSERT_DATE_COLUMN, logger)
        generateReport(historicalTransactionsDf, reportDataPath, glueContext, logger)
    }
}
