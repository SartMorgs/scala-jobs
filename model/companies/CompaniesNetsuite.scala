package silverLayer.netsuiteIntegration.companies

import com.amazonaws.services.glue.log.GlueLogger
import org.apache.spark.sql.DataFrame
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{concat,lit,col}
import silverLayer.netsuiteIntegration.model.AllRegionsTable

class CompaniesNetsuite extends AllRegionsTable with CompaniesSourceData {
    val YEAR_COLUMN = "year"
    val MONTH_COLUMN = "month"
    val DAY_COLUMN = "day"

    val CREATED_AT_COLUMN = "created_at"
    val START_OPERATION_COLUMN = "start_operation"
    val ID_CLARA_CONTRACT_COLUMN = "id_clara_contract"
    val MINSAIT_CONTRACT_ID_COLUMN = "minsait_contract_id"

    val MX_COUNTRY = "MX"
    val BR_COUNTRY = "BR"
    val CO_COUNTRY = "CO"

    def filterActiveCompanies(clientsDf: DataFrame, logger: GlueLogger): DataFrame = {
        val filteredActiveClientsDf = clientsDf.filter("status == '0'").na.drop(Seq(MINSAIT_CONTRACT_ID_COLUMN))
        logger.info("Remove non active clients!")
        filteredActiveClientsDf
    }

    def unionAllRegionsData(mxDf: DataFrame, brDf: DataFrame, coDf: DataFrame, logger: GlueLogger): DataFrame = {
        logger.info("Generated one dataframe from union between all regions dataframes!")
        mxDf.union(brDf).union(coDf)
    }

    def selectColumns(sourceDf: DataFrame, country: String, logger: GlueLogger): DataFrame = {
        val countryWithHyphen = country + "-"
        val colsToSelect = Seq(
            s"$START_OPERATION_COLUMN as $CREATED_AT_COLUMN",
            MINSAIT_CONTRACT_ID_COLUMN,
            "legal_name as company_name",
            "tax_identify as tax_id"
        )
        logger.info("Select columns for clients report!")
        sourceDf
            .selectExpr(colsToSelect: _*)
            .withColumn(ID_CLARA_CONTRACT_COLUMN, concat(lit(countryWithHyphen),col(MINSAIT_CONTRACT_ID_COLUMN)))
            .withColumn("country", lit(country))
            .drop(MINSAIT_CONTRACT_ID_COLUMN)
    }

    def getTable(sourcePath: String, country: String, logger: GlueLogger): DataFrame = {
        val companiesDf = DeltaTable.forPath(sourcePath).toDF
        val activeCompaniesDF = filterActiveCompanies(companiesDf, logger)
        val reportColumnsDF = selectColumns(activeCompaniesDF, country, logger)
        reportColumnsDF
    }
}
