package silverLayer.netsuiteIntegration.financeEconomicConcepts

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import com.amazonaws.services.glue.log.GlueLogger
import org.apache.spark.sql.functions.{concat,col,max}
import org.apache.spark.sql.types.IntegerType

class FinanceEconomicConcepts {
    val YEAR_COLUMN = "year"
    val MONTH_COLUMN = "month"
    val DAY_COLUMN = "day"

    val ECONOMIC_CONCEPT_COLUMN = "economic_concept"
    val NORMAL_CORRECTORA_COLUMN = "normal/correctora"
    val NEW_NORMAL_CORRECTORA_COLUMN = "normal_correctora"
    val REGISTER_TYPE_COLUMN = "registertype"

    def getLastData(sourceDf: DataFrame, logger: GlueLogger): DataFrame = {
        val w = Window.partitionBy(ECONOMIC_CONCEPT_COLUMN, NORMAL_CORRECTORA_COLUMN)
        val dateNumberColumn = "date_number"
        val maxDateNumberColumn = "max_date"
        logger.info("Get most recent data from finance economic concepts!")
        sourceDf
            .withColumn(dateNumberColumn, concat(col(YEAR_COLUMN), col(MONTH_COLUMN), col(DAY_COLUMN)).cast(IntegerType))
            .withColumn(maxDateNumberColumn, max(dateNumberColumn).over(w))
            .where(s"$dateNumberColumn == $maxDateNumberColumn")
            .distinct()
            .drop(maxDateNumberColumn)
            .drop(dateNumberColumn)
            .drop(YEAR_COLUMN)
            .drop(MONTH_COLUMN)
            .drop(DAY_COLUMN)
    }
}
