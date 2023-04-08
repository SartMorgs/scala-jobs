package silverLayer.netsuiteIntegration.transactions

import com.amazonaws.services.glue.log.GlueLogger
import org.apache.spark.sql.DataFrame
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{lit,concat,col}

class EconomicConceptsOperationsTransactions extends TransactionsNetsuite with TransactionsSourceData {

    def selectColumns(sourceDf: DataFrame, country: String, logger: GlueLogger): DataFrame = {
        val countryWithHyphen = country + "-"
        val colsToSelect = Seq(
            INSERT_DATE_COLUMN, s"$UUID_COLUMN as new_$UUID_COLUMN",
            s"interests_calculation_last_date as $DATE_COLUMN",
            s"file_date as $ACCOUNTING_DATE_COLUMN",
            s"economic_concept_code as $INVOICE_COLUMN",
            MINSAIT_CONTRACT_ID_COLUMN,
            s"economic_concept_code_description as $INVOICE_DESCRIPTION_COLUMN",
            s"economic_concept_gross_amount as $INVOICE_AMOUNT_COLUMN"
        )
        logger.info("Select columns for economic_concepts_operations table!")
        sourceDf
            .selectExpr(colsToSelect: _*)
            .withColumn(TYPE_COLUMN, lit("EC"))
            .withColumn(UUID_COLUMN, concat(lit("ec-"), col(s"new_$UUID_COLUMN")))
            .withColumn(ID_CLIENT_COLUMN, concat(lit(countryWithHyphen),col(MINSAIT_CONTRACT_ID_COLUMN)))
            .drop(s"new_$UUID_COLUMN")
            .drop(MINSAIT_CONTRACT_ID_COLUMN)
    }

    def getTable(sourcePath: String, country: String, logger: GlueLogger): DataFrame = {
        val tableName = "economic_concepts_operations"
        val economicConceptsDf = DeltaTable.forPath(s"$sourcePath/$tableName").toDF
        val economicConceptsReportColumnsDf = selectColumns(economicConceptsDf, country, logger)
        logger.info("Generated economic concepts operations table!")
        generateTwoColumnsOfAmount(economicConceptsReportColumnsDf, logger)
    }

    def getAllRegions(
        mxSourcePath: String, brSourcePath: String, coSourcePath: String, logger: GlueLogger
    ): DataFrame = {
        val mxEconomicConceptsReportDf = getTable(mxSourcePath, MX_COUNTRY, logger)
        val brEconomicConceptsReportDf = getTable(brSourcePath, BR_COUNTRY, logger)
        val coEconomicConceptsReportDf = getTable(coSourcePath, CO_COUNTRY, logger)

        logger.info("Generated economic concepts operations table for all reagions!")
        unionAllRegionsData(mxEconomicConceptsReportDf, brEconomicConceptsReportDf, coEconomicConceptsReportDf, logger)
    }
}
