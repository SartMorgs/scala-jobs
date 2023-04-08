package bronzeLayer.dataHandleFromDms

import com.amazonaws.services.glue.{DataSource, DynamicFrame, GlueContext}
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.util.{Job, JsonOptions}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, dayofmonth, format_string, lit, max, month, udf, year}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataTypes

import scala.collection.JavaConverters.mapAsJavaMapConverter
import utils.anonymization.DataAnonymization
import utils.datacatalog.DataCatalogUtils


object HandleDmsData {
    var encryptUDF: UserDefinedFunction = _
    var anonymization: DataAnonymization = _

    val CORRECT_STRING = "CORRECTED"
    val OP_STRING = "Op"
    val C_STRING = "C"
    val TIMESTAMP_STRING = "TIMESTAMP"

    def initializingSpark(logger: GlueLogger): GlueContext = {
        logger.info("Initializing Spark and Glue Context")

        val conf = new SparkConf()
            .setAppName("GlueHandleDmsData")
            .set("fs.s3a.endpoint", "s3.amazonaws.com")
            .set("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .set("spark.databricks.hive.metastore.glueCatalog.enabled", "true")
            .set("spark.sql.legacy.parquet.int96RebaseModeInRead", CORRECT_STRING)
            .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", CORRECT_STRING)
            .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", CORRECT_STRING)
            .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", CORRECT_STRING)

        val sc: SparkContext = new SparkContext(conf)
        sc.setLogLevel("INFO")

        val glueContext: GlueContext = new GlueContext(sc)

        glueContext
  }

    def getSourceDf(glueContext: GlueContext, tablePath: String): DataFrame = {
        val source: DataSource = glueContext.getSourceWithFormat(
            connectionType = "s3",
            options = JsonOptions(Map("paths" -> Seq(tablePath))),
            transformationContext = "cdc",
            format = "parquet",
            formatOptions = JsonOptions.empty
        )

        val dynamicFrameCdc: DynamicFrame = source.getDynamicFrame()
        dynamicFrameCdc.toDF()
    }

    def generateCdc(sourceDf: DataFrame, primaryKeyList: Array[String]): DataFrame = {
        val w = Window.partitionBy(primaryKeyList.map(col):_*)
        sourceDf
            .withColumn("tmp_ts_ms_max", max("transact_seq").over(w))
            .where("transact_seq == tmp_ts_ms_max")
            .drop("tmp_ts_ms_max")
            .drop("transact_seq")
    }

    def getCdcDataFrame(glueContext: GlueContext, tablePath: String, primaryKeyList: Array[String], logger: GlueLogger): DataFrame = {
        var sourceDf = getSourceDf(glueContext, tablePath)
        sourceDf = sourceDf.na.fill(C_STRING, Seq(OP_STRING))

        if (primaryKeyList(0) != ""){
            logger.info("Table with primary key!")
            sourceDf = generateCdc(sourceDf, primaryKeyList)
        }

        logger.info("Created dataframe from source data!")
        sourceDf
    }

    def getDataframeWithDataFieldsFromTimestamp(sourceDf: DataFrame): DataFrame = {
        sourceDf
            .withColumn("year", year(col(TIMESTAMP_STRING)).cast("String"))
            .withColumn("month", format_string("%02d", month(col(TIMESTAMP_STRING))))
            .withColumn("day", format_string("%02d", dayofmonth(col(TIMESTAMP_STRING))))
    }

    def encryptData(sourceDataFrame: DataFrame, columnToAnonymize: String): DataFrame = {
        sourceDataFrame.withColumn(columnToAnonymize, encryptUDF(col(columnToAnonymize).cast(DataTypes.StringType)))
    }

    def processMergeDmsData(
        glueContext: GlueContext,
        newData: DataFrame,
        targetPath: String,
        tableName: String,
        glueDatabase: String,
        primaryKeyList: String,
        logger: GlueLogger): Unit = {
        val datacatalogUtils = new DataCatalogUtils(glueContext)
        val newDataWithDate = getDataframeWithDataFieldsFromTimestamp(newData)
        datacatalogUtils.writeDeltaTableForIncrementalData(newDataWithDate, glueDatabase, tableName, primaryKeyList)
        logger.info("Data processed successfully!")
    }

    def run(args: Map[String, String]): Unit = {
        val logger = new GlueLogger
        val glueContext = initializingSpark(logger)
        val spark = glueContext.getSparkSession

        val ANONYMIZATION_SECRET_NAME = args("ANONYMIZATION_SECRET_NAME")
        val JOB_NAME = args("JOB_NAME")
        val TABLE_NAME = args("TABLE_NAME")
        val TABLE_SCHEMA = args("TABLE_SCHEMA")
        val GLUE_DATABASE = args("GLUE_DATABASE")
        val TABLE_PRIMARY_KEYS = args("TABLE_PRIMARY_KEYS_LIST").replace("\"", "")
        val TABLE_ANONYMIZATION_COLUMNS = args("TABLE_ANONYMIZATION_COLUMNS_LIST").replace("\"", "")

        val tablePath = s"${args("SOURCE_DATA_PATH")}/$TABLE_SCHEMA/$TABLE_NAME"
        val targetPath = s"${args("TARGET_DATA_PATH")}/$TABLE_SCHEMA/$TABLE_NAME"
        val tablePrimaryKeys = TABLE_PRIMARY_KEYS.split(",")
        val tableAnonymizationColumns = TABLE_ANONYMIZATION_COLUMNS.split(",")
        anonymization = new DataAnonymization(ANONYMIZATION_SECRET_NAME)
        var dfCdc = spark.emptyDataFrame

        logger.info("Job init for bookmark")
        Job.init(JOB_NAME, glueContext, args.asJava)

        if (tablePrimaryKeys(0) != ""){
            try {
                dfCdc = getCdcDataFrame(glueContext, tablePath, tablePrimaryKeys, logger)
            } catch {
                case _: Exception => logger.info("There isn't data into this folder/table!")
            }

            if (!dfCdc.head(1).isEmpty) {
                encryptUDF = udf(anonymization.encrypt _)
                if (tableAnonymizationColumns(0) != "") {
                    tableAnonymizationColumns.foreach { anonymizeColumn =>
                        dfCdc = encryptData(dfCdc.na.fill("", Seq(anonymizeColumn)), anonymizeColumn)
                    }
                }

                processMergeDmsData(glueContext, dfCdc, targetPath, TABLE_NAME, GLUE_DATABASE, TABLE_PRIMARY_KEYS, logger)
                logger.info("Created manifest successfully!")
            }
        }

        Job.commit()
        logger.info("Job commit for bookmark")
    }
}
