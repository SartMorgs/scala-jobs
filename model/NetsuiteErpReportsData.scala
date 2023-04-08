package silverLayer.netsuiteIntegration

import com.amazonaws.services.glue.GlueContext
import io.delta.tables.DeltaTable
import com.amazonaws.services.glue.log.GlueLogger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col,year,month,dayofmonth,current_date,lit,date_sub}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataTypes
import utils.delta.DeltaTableUtils
import silverLayer.netsuiteIntegration.financeEconomicConcepts.FinanceEconomicConcepts
import silverLayer.netsuiteIntegration.transactions.{TransactionsNetsuite,OperativeOperationsTransactions,EconomicConceptsOperationsTransactions}
import silverLayer.netsuiteIntegration.companies.CompaniesNetsuite

object NetsuiteErpReportsData {
    private val YEAR_COLUMN = "year"
    private val MONTH_COLUMN = "month"
    private val DAY_COLUMN = "day"
    private val SOURCE_DATA_PATH_ARG = "SOURCE_DATA_PATH"
    private val TARGET_DATA_PATH_ARG = "TARGET_DATA_PATH"
    private val NETSUITE_ERP_DATABASE_NAME_ARG = "NETSUITE_ERP_DATABASE_NAME"

    private val ACTIVE_CLIENTS_TABLE = "active_clients"
    private val TRANSACTIONS_TABLE = "transactions"
    private val TRANSACTIONS_OUT_OF_REPORT_TABLE = "transactions_out_of_report"

    def initializingSpark(logger: GlueLogger): GlueContext = {
        logger.info("Initializing Spark and Glue Context")

        val correctedString = "CORRECTED"
        val conf = new SparkConf()
        .setAppName("GlueNetsuiteIntegration")
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

    def generateTransactionsTable(
        transactionsSourceDf: DataFrame, transactionsNetsuite: TransactionsNetsuite,
        financeEconomicConcepts: FinanceEconomicConcepts, financeEcSourceDf: DataFrame, logger: GlueLogger
    ): DataFrame = {
        val colsToSelect = Seq(
            col(transactionsNetsuite.INSERT_DATE_COLUMN),
            col(transactionsNetsuite.UUID_COLUMN),
            col(transactionsNetsuite.DATE_COLUMN),
            col(transactionsNetsuite.ACCOUNTING_DATE_COLUMN),
            col(transactionsNetsuite.INVOICE_COLUMN),
            col("ec_description_"),
            col(transactionsNetsuite.TYPE_COLUMN),
            col(transactionsNetsuite.ID_CLIENT_COLUMN),
            col(transactionsNetsuite.AMOUNT_DEBIT_COLUMN),
            col(transactionsNetsuite.AMOUNT_CREDIT_COLUMN),
            col(financeEconomicConcepts.NORMAL_CORRECTORA_COLUMN),
            col(transactionsNetsuite.DEBIT_ACCOUNT_COLUMN),
            col(transactionsNetsuite.CREDIT_ACCOUNT_COLUMN),
            col(YEAR_COLUMN),
            col(MONTH_COLUMN),
            col(DAY_COLUMN)
        )
        val joinDf = transactionsSourceDf
            .join(financeEcSourceDf)
            .filter(transactionsSourceDf(transactionsNetsuite.INVOICE_COLUMN) === financeEcSourceDf(financeEconomicConcepts.ECONOMIC_CONCEPT_COLUMN) &&
                transactionsSourceDf(transactionsNetsuite.TYPE_COLUMN) === financeEcSourceDf(financeEconomicConcepts.REGISTER_TYPE_COLUMN))
            .select(colsToSelect:_*)
            .withColumnRenamed(financeEconomicConcepts.NORMAL_CORRECTORA_COLUMN, transactionsNetsuite.NEW_NORMAL_CORRECTORA_COLUMN)
            .withColumnRenamed("ec_description_", transactionsNetsuite.INVOICE_DESCRIPTION_COLUMN)
        val positiveAmount = transactionsNetsuite.getTransactionsWithNormalAccount(joinDf, logger)
        val negativeAmount = transactionsNetsuite.getTransactionsWithCorrectoraAccount(joinDf, logger)
        logger.info("Generated one dataframe with account ID!")
        positiveAmount.union(negativeAmount)
    }

    def getSourceTransactions(
        mxSourcePath: String,
        brSourcePath: String,
        coSourcePath: String,
        netsuiteErpDatabaseName: String,
        glueContext: GlueContext,
        logger: GlueLogger
    ): DataFrame = {
        val opTransactions = new OperativeOperationsTransactions
        val ecTransactions = new EconomicConceptsOperationsTransactions
        val opAllRegionsDf = opTransactions.getAllRegions(mxSourcePath, brSourcePath, coSourcePath, logger)
        val ecAllRegionsDf = ecTransactions.getAllRegions(mxSourcePath, brSourcePath, coSourcePath, logger)
        logger.info("Get source transactions dataframe for all regions!")
        opAllRegionsDf.union(ecAllRegionsDf)
            .withColumn(YEAR_COLUMN, year(col(opTransactions.INSERT_DATE_COLUMN)).cast(DataTypes.StringType))
            .withColumn(MONTH_COLUMN, month(col(opTransactions.INSERT_DATE_COLUMN)).cast(DataTypes.StringType))
            .withColumn(DAY_COLUMN, dayofmonth(col(opTransactions.INSERT_DATE_COLUMN)).cast(DataTypes.StringType))
    }

    def getTransactionsOutOfReport(
        sourceDf: DataFrame, financeEcSourceDf: DataFrame, normalCorrectora: String,
        transactionsNetsuite: TransactionsNetsuite, financeEconomicConcepts: FinanceEconomicConcepts
    ): DataFrame = {
        val colsToSelect = Seq(
            transactionsNetsuite.INSERT_DATE_COLUMN,
            transactionsNetsuite.UUID_COLUMN,
            transactionsNetsuite.DATE_COLUMN,
            transactionsNetsuite.INVOICE_COLUMN,
            transactionsNetsuite.INVOICE_DESCRIPTION_COLUMN,
            transactionsNetsuite.TYPE_COLUMN,
            transactionsNetsuite.ID_CLIENT_COLUMN,
            transactionsNetsuite.AMOUNT_DEBIT_COLUMN,
            transactionsNetsuite.AMOUNT_CREDIT_COLUMN,
            YEAR_COLUMN,
            MONTH_COLUMN,
            DAY_COLUMN
        )
        val financeFiltered = financeEcSourceDf
            .withColumnRenamed(financeEconomicConcepts.NORMAL_CORRECTORA_COLUMN, financeEconomicConcepts.NEW_NORMAL_CORRECTORA_COLUMN)
            .filter(financeEconomicConcepts.NEW_NORMAL_CORRECTORA_COLUMN + s"== '$normalCorrectora'")
        sourceDf
            .join(financeFiltered,
                sourceDf(transactionsNetsuite.INVOICE_COLUMN) === financeFiltered(financeEconomicConcepts.ECONOMIC_CONCEPT_COLUMN) &&
                sourceDf(transactionsNetsuite.TYPE_COLUMN) === financeFiltered(financeEconomicConcepts.REGISTER_TYPE_COLUMN), "left")
            .filter(col(financeEconomicConcepts.ECONOMIC_CONCEPT_COLUMN).isNull &&
                col(financeEconomicConcepts.REGISTER_TYPE_COLUMN).isNull)
            .selectExpr(colsToSelect:_*)
    }

    def generateTransactionsOutOfReport(
        transactionsSourceDf: DataFrame, financeEcSourceDf: DataFrame,
        transactionsNetsuite: TransactionsNetsuite, financeEconomicConcepts: FinanceEconomicConcepts,
        logger: GlueLogger
    ): DataFrame = {
        val normalTransactions = transactionsSourceDf
            .filter(col(transactionsNetsuite.AMOUNT_DEBIT_COLUMN) >= 0 || col(transactionsNetsuite.AMOUNT_CREDIT_COLUMN) >= 0)
        val correctoraTransactions = transactionsSourceDf
            .filter(col(transactionsNetsuite.AMOUNT_DEBIT_COLUMN) < 0 || col(transactionsNetsuite.AMOUNT_CREDIT_COLUMN) < 0)
        logger.info("Generated dataframe with transactions out of report!")
        val normalTransactionsOutOfReport = getTransactionsOutOfReport(
            normalTransactions,financeEcSourceDf, "Normal", transactionsNetsuite, financeEconomicConcepts)
        val correctoraTransactionsOutOfReport = getTransactionsOutOfReport(
            correctoraTransactions, financeEcSourceDf, "Correctora", transactionsNetsuite, financeEconomicConcepts)
        normalTransactionsOutOfReport.union(correctoraTransactionsOutOfReport)
    }

    def getTransactionsReprocessed(
        transactionsDf: DataFrame, transactionsOutOfReportDf: DataFrame, transactionsNetsuite: TransactionsNetsuite
    ): DataFrame = {
        val newUuidColumn = "new_" + transactionsNetsuite.UUID_COLUMN
        val newInvoiceColumn = "new_" + transactionsNetsuite.INVOICE_COLUMN
        val transacationsOutOfReportUuidDf = transactionsOutOfReportDf
            .select(transactionsNetsuite.UUID_COLUMN, transactionsNetsuite.INVOICE_COLUMN)
            .distinct
            .withColumnRenamed(transactionsNetsuite.UUID_COLUMN, newUuidColumn)
            .withColumnRenamed(transactionsNetsuite.INVOICE_COLUMN, newInvoiceColumn)
        transactionsDf
            .join(transacationsOutOfReportUuidDf)
            .filter(transactionsDf(transactionsNetsuite.UUID_COLUMN) === transacationsOutOfReportUuidDf(newUuidColumn) &&
                transactionsDf(transactionsNetsuite.INVOICE_COLUMN) === transacationsOutOfReportUuidDf(newInvoiceColumn))
            .drop(newUuidColumn, newInvoiceColumn)
            .filter(transactionsDf(transactionsNetsuite.DATE_COLUMN).gt(lit("2023-01-01")))
    }

    def changeDataForYesterday(sourceDf: DataFrame, dateField: String): DataFrame = {
        sourceDf.withColumn(dateField, date_sub(current_date(), 1))
    }

    def reprocessedTransactions(
        transactionsPath: String,
        transactionsOutOfReportPath: String,
        transactionsNetsuite: TransactionsNetsuite,
        glueContext: GlueContext,
        logger: GlueLogger
    ): Unit = {
        val deltaTableUtils = new DeltaTableUtils(glueContext.getSparkSession)

        val transactionsDf = deltaTableUtils.returnDf(transactionsPath)
        val transactionsOutOfReportDf = deltaTableUtils.returnDf(transactionsOutOfReportPath)

        if(transactionsOutOfReportDf.isEmpty){
            logger.info("There is not transactions out of report on silver layer!")
        } else{
            val reprocessedTransactionsDf = getTransactionsReprocessed(transactionsDf, transactionsOutOfReportDf, transactionsNetsuite)
            val primaryKey = transactionsNetsuite.UUID_COLUMN + "," + transactionsNetsuite.ACCOUNT_ID_COLUMN
            val reprocessedTransactionsWithNewDataDf = changeDataForYesterday(reprocessedTransactionsDf, transactionsNetsuite.INSERT_DATE_COLUMN)
            deltaTableUtils.performUpsert(transactionsPath, reprocessedTransactionsWithNewDataDf, primaryKey)

            val deleteTransactionsOutOfReport = getTransactionsReprocessed(transactionsOutOfReportDf, transactionsDf, transactionsNetsuite)
            deltaTableUtils.performDelete(transactionsOutOfReportPath, deleteTransactionsOutOfReport, transactionsNetsuite.UUID_COLUMN)
            logger.info("Reprocessed transactions on silver layer!")
        }
    }

    def activeClients(args: Map[String, String]): Unit = {
        val logger = new GlueLogger
        val glueContext = initializingSpark(logger)
        val companiesNetsuite = new CompaniesNetsuite
        val deltaUtils = new DeltaTableUtils(glueContext.getSparkSession)

        val sourceDataPath = args(SOURCE_DATA_PATH_ARG)
        val environment = args("STAGE")
        val targetPath = (s"${args(TARGET_DATA_PATH_ARG)}/erp_reports/netsuite/active_clients")

        val database = "client-services"
        val schemaAndTable = "client_service_schema/companies"
        val mxSourcePath = s"$sourceDataPath/$database/mx/$schemaAndTable"
        val brSourcePath = s"$sourceDataPath/$database/br/$schemaAndTable"
        val coSourcePath = s"$sourceDataPath/$database/co/$schemaAndTable"

        val mxReportColumnsDF = companiesNetsuite.getTable(mxSourcePath, "MX", logger)
        val brReportColumnsDf = companiesNetsuite.getTable(brSourcePath, "BR", logger)
        val coReportColumnsDf = companiesNetsuite.getTable(coSourcePath, "CO", logger)

        var activeClientsReportDf = companiesNetsuite.unionAllRegionsData(
            mxReportColumnsDF, brReportColumnsDf, coReportColumnsDf, logger)
            .withColumn(YEAR_COLUMN, year(col(companiesNetsuite.CREATED_AT_COLUMN)).cast(DataTypes.StringType))
            .withColumn(MONTH_COLUMN, month(col(companiesNetsuite.CREATED_AT_COLUMN)).cast(DataTypes.StringType))
            .withColumn(DAY_COLUMN, dayofmonth(col(companiesNetsuite.CREATED_AT_COLUMN)).cast(DataTypes.StringType))

        val primaryKey = companiesNetsuite.ID_CLARA_CONTRACT_COLUMN
        deltaUtils.performUpsert(targetPath, activeClientsReportDf, primaryKey, true)
        logger.info("Generated active clients on silver layer!")
    }

    def transactions(args: Map[String, String]): Unit = {
        val logger = new GlueLogger
        val glueContext = initializingSpark(logger)
        val financeEconomicConcepts = new FinanceEconomicConcepts
        val transactionsNetsuite = new TransactionsNetsuite
        val deltaUtils = new DeltaTableUtils(glueContext.getSparkSession)

        val sourceDataPath = args(SOURCE_DATA_PATH_ARG)
        val netsuiteErpDatabaseName = args(NETSUITE_ERP_DATABASE_NAME_ARG)
        val transactionsOutOfReportPath = (s"${args(TARGET_DATA_PATH_ARG)}/erp_reports/netsuite/transactions_out_of_report")

        val database = "operations"
        val schema = "public"
        val financeEcTableName = "finance_economic_concepts"
        val mxSourcePath = s"$sourceDataPath/$database/mx/$schema"
        val brSourcePath = s"$sourceDataPath/$database/br/$schema"
        val coSourcePath = s"$sourceDataPath/$database/co/$schema"
        val googleSheetsPath =  s"$sourceDataPath/google_sheets"
        val transactionsPath = (s"${args(TARGET_DATA_PATH_ARG)}/erp_reports/netsuite/transactions")

        val financeEcDf = deltaUtils.readDeltaTable(s"$googleSheetsPath/$financeEcTableName").toDF
        val mostRecentFinanceEcDf = financeEconomicConcepts.getLastData(financeEcDf, logger)

        val mostRecentDataDf = getSourceTransactions(mxSourcePath, brSourcePath, coSourcePath, netsuiteErpDatabaseName, glueContext, logger)
        val transactionsMirroredDf = transactionsNetsuite.mirrorTransactions(mostRecentDataDf, logger)
        val transactionsReport = generateTransactionsTable(transactionsMirroredDf, transactionsNetsuite, financeEconomicConcepts, mostRecentFinanceEcDf, logger)
        val transactionsWithAbsoluteValues = transactionsNetsuite.getTransactionsWithAbsoluteValue(transactionsReport, logger)
        val transactionsIgnoringNaValues = transactionsNetsuite.getTransactionsIgnoringNaValues(transactionsWithAbsoluteValues, logger)

        val primaryKey = transactionsNetsuite.UUID_COLUMN + "," + transactionsNetsuite.ACCOUNT_ID_COLUMN
        deltaUtils.performUpsert(transactionsPath, transactionsIgnoringNaValues, primaryKey, true)
        logger.info("Generated transactions on silver layer!")

        reprocessedTransactions(transactionsPath, transactionsOutOfReportPath, transactionsNetsuite,
            glueContext, logger)
    }

    def transactionsOutOfReport(args: Map[String, String]): Unit = {
        val logger = new GlueLogger
        val glueContext = initializingSpark(logger)
        val financeEconomicConcepts = new FinanceEconomicConcepts
        val transactionsNetsuite = new TransactionsNetsuite
        val deltaUtils = new DeltaTableUtils(glueContext.getSparkSession)

        val sourceDataPath = args(SOURCE_DATA_PATH_ARG)
        val netsuiteErpDatabaseName = args(NETSUITE_ERP_DATABASE_NAME_ARG)
        val targetPath = (s"${args(TARGET_DATA_PATH_ARG)}/erp_reports/netsuite/transactions_out_of_report")

        val database = "operations"
        val schema = "public"
        val financeEcTableName = "finance_economic_concepts"
        val mxSourcePath = s"$sourceDataPath/$database/mx/$schema"
        val brSourcePath = s"$sourceDataPath/$database/br/$schema"
        val coSourcePath = s"$sourceDataPath/$database/co/$schema"
        val googleSheetsPath =  s"$sourceDataPath/google_sheets"

        val financeEcDf = deltaUtils.readDeltaTable(s"$googleSheetsPath/$financeEcTableName").toDF
        val mostRecentFinanceEcDf = financeEconomicConcepts.getLastData(financeEcDf, logger)

        val mostRecentDataDf = getSourceTransactions(mxSourcePath, brSourcePath, coSourcePath, netsuiteErpDatabaseName, glueContext, logger)
        val transactionsOutOfReportDf = generateTransactionsOutOfReport(
            mostRecentDataDf, mostRecentFinanceEcDf, transactionsNetsuite, financeEconomicConcepts, logger)

        deltaUtils.performUpsert(targetPath, transactionsOutOfReportDf, transactionsNetsuite.UUID_COLUMN, true)
        logger.info("Generated transactions out of report on silver layer!")
    }
}
