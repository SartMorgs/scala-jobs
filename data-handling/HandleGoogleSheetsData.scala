package bronzeLayer.dataHandleFromGoogleSheets

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.log.GlueLogger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_date,year,month,dayofmonth}
import org.apache.spark.sql.types.DataTypes
import com.amazonaws.services.glue.util.{Job, JsonOptions}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters.mapAsJavaMapConverter
import utils.delta.DeltaTableUtils
import utils.datacatalog.DataCatalogUtils

object HandleGoogleSheetsData {
    def initializingSpark(logger: GlueLogger): GlueContext = {
        logger.info("Initializing Spark and Glue Context")

        val correctedString = "CORRECTED"
        val conf = new SparkConf()
        .setAppName("GlueHandleGoogleSheetsData")
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

    def run(args: Map[String, String]): Unit = {
        val logger = new GlueLogger
        val glueContext = initializingSpark(logger)
        val deltaUtils = new DeltaTableUtils(glueContext.getSparkSession)
        val datacatalogUitls = new DataCatalogUtils(glueContext)

        val JOB_NAME = args("JOB_NAME")
        val SOURCE_DATA_PATH = args("SOURCE_DATA_PATH")
        val TARGET_DATABASE_NAME = args("TARGET_DATABASE_NAME")
        val TABLE_NAME = args("TABLE_NAME")
        val RAW_LAYER_PATH = args("RAW_LAYER_PATH")

        logger.info("Job init for bookmark")
        Job.init(JOB_NAME, glueContext, args.asJava)

        val sourceDf = datacatalogUitls.loadDataFromLandingZone(SOURCE_DATA_PATH, "csv")

        if (sourceDf.isEmpty){
            logger.info("Source dataframe is empty!")
        } else {
            val sourceDfWithDateColumns = sourceDf
                .withColumn("year", year(current_date()).cast(DataTypes.StringType))
                .withColumn("month", month(current_date()).cast(DataTypes.StringType))
                .withColumn("day", dayofmonth(current_date()).cast(DataTypes.StringType))
            datacatalogUitls.writeDeltaTableForFullDataLoad(sourceDfWithDateColumns, TARGET_DATABASE_NAME, TABLE_NAME)
        }

        logger.info("Job commit for bookmark")
        Job.commit()
    }
}
