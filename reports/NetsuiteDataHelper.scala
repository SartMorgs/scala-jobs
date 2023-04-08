package goldLayer.netsuiteReports

import org.apache.spark.sql.DataFrame
import com.amazonaws.services.glue.log.GlueLogger
import org.apache.spark.sql.functions.{current_date,year,month,dayofmonth,lit,date_add}
import org.apache.spark.sql.types.DataTypes

class NetsuiteDataHelper {
    val YEAR_COLUMN = "year"
    val MONTH_COLUMN = "month"
    val DAY_COLUMN = "day"

    val DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"

    def getDataWithCurrentDataAsYearMonthDay(sourceDf: DataFrame, logger: GlueLogger): DataFrame = {
        logger.info("Generated dataframe with year, month, day columns with current datetime!")
        sourceDf
            .withColumn(YEAR_COLUMN, year(current_date()).cast(DataTypes.StringType))
            .withColumn(MONTH_COLUMN, month(current_date()).cast(DataTypes.StringType))
            .withColumn(DAY_COLUMN, dayofmonth(current_date()).cast(DataTypes.StringType))
    }

    def getDataForYesterday(sourceDf: DataFrame, dateColumnName: String): DataFrame = {
        if(sourceDf.isEmpty){
            sourceDf
        } else {
            val currentDateColumnName = "current_date"
            sourceDf
                .withColumn(currentDateColumnName, lit(date_add(current_date(),-1)))
                .filter(s"$dateColumnName >= $currentDateColumnName")
                .drop(currentDateColumnName)
                .drop(YEAR_COLUMN, MONTH_COLUMN, DAY_COLUMN)
        }
    }
}
