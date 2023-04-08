package silverLayer.netsuiteIntegration.transactions

import com.amazonaws.services.glue.log.GlueLogger
import org.apache.spark.sql.DataFrame
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{lit,concat,col}

class OperativeOperationsTransactions extends TransactionsNetsuite with TransactionsSourceData {

    def selectColumns(sourceDf: DataFrame, country: String, logger: GlueLogger): DataFrame = {
        val countryWithHyphen = country + "-"
        val colsToSelect = Seq(
            INSERT_DATE_COLUMN,
            s"$UUID_COLUMN as new_$UUID_COLUMN",
            s"invoice_date as $DATE_COLUMN",
            s"accounting_operation_date as $ACCOUNTING_DATE_COLUMN",
            s"invoice_type as $INVOICE_COLUMN",
            MINSAIT_CONTRACT_ID_COLUMN,
            s"invoice_type_description as $INVOICE_DESCRIPTION_COLUMN",
            INVOICE_AMOUNT_COLUMN
        )
        logger.info("Select columns for operative_operations table!")
        sourceDf
            .selectExpr(colsToSelect: _*)
            .withColumn(TYPE_COLUMN, lit("OP"))
            .withColumn(UUID_COLUMN, concat(lit("op-"), col(s"new_$UUID_COLUMN")))
            .withColumn(ID_CLIENT_COLUMN, concat(lit(countryWithHyphen),col(MINSAIT_CONTRACT_ID_COLUMN)))
            .drop(s"new_$UUID_COLUMN")
            .drop(MINSAIT_CONTRACT_ID_COLUMN)
    }

    def getTable(sourcePath: String, country: String, logger: GlueLogger): DataFrame = {
        val tableName = "operative_operations"
        val operativeOperationsDf = DeltaTable.forPath(s"$sourcePath/$tableName").toDF
        val operativeOperationsReportColumnsDf = selectColumns(operativeOperationsDf, country, logger)
        logger.info("Generated operative operations table!")
        generateTwoColumnsOfAmount(operativeOperationsReportColumnsDf, logger)
    }

    def getAllRegions(
        mxSourcePath: String, brSourcePath: String, coSourcePath: String, logger: GlueLogger
    ): DataFrame = {
        val mxOperativeOperationsReportDf = getTable(mxSourcePath, MX_COUNTRY, logger)
        val brOperativeOperationsReportDf = getTable(brSourcePath, BR_COUNTRY, logger)
        val coOperativeOperationsReportDf = getTable(coSourcePath, CO_COUNTRY, logger)

        logger.info("Generated operative operations table for all reagions!")
        unionAllRegionsData(
            mxOperativeOperationsReportDf, brOperativeOperationsReportDf, coOperativeOperationsReportDf, logger
        )
    }
}
