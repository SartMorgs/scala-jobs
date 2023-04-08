package silverLayer.netsuiteIntegration.transactions

import com.amazonaws.services.glue.log.GlueLogger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col,lit}
import silverLayer.netsuiteIntegration.model.AllRegionsTable

class TransactionsNetsuite extends AllRegionsTable {
    val YEAR_COLUMN = "year"
    val MONTH_COLUMN = "month"
    val DAY_COLUMN = "day"

    val INSERT_DATE_COLUMN = "insert_date"
    val ACCOUNTING_DATE_COLUMN = "accounting_date"
    val UUID_COLUMN = "uuid"
    val DATE_COLUMN = "date"
    val INVOICE_COLUMN = "invoice"
    val MINSAIT_CONTRACT_ID_COLUMN = "minsait_contract_id"
    val INVOICE_DESCRIPTION_COLUMN = "invoice_description"
    val INVOICE_AMOUNT_COLUMN = "invoice_amount"
    val TYPE_COLUMN = "type"
    val ID_CLIENT_COLUMN = "id_client"
    val AMOUNT_CREDIT_COLUMN = "amount_c"
    val AMOUNT_DEBIT_COLUMN = "amount_d"
    val DEBIT_ACCOUNT_COLUMN = "debit_account"
    val ACCOUNT_ID_COLUMN = "account_id"
    val CREDIT_ACCOUNT_COLUMN = "credit_account"
    val NEW_NORMAL_CORRECTORA_COLUMN = "normal_correctora"

    val MX_COUNTRY = "MX"
    val BR_COUNTRY = "BR"
    val CO_COUNTRY = "CO"

    def generateTwoColumnsOfAmount(transactionsDf: DataFrame, logger: GlueLogger): DataFrame = {
        val colsToSelect = Seq(
            INSERT_DATE_COLUMN,
            UUID_COLUMN,
            DATE_COLUMN,
            ACCOUNTING_DATE_COLUMN,
            INVOICE_COLUMN,
            INVOICE_DESCRIPTION_COLUMN,
            TYPE_COLUMN,
            ID_CLIENT_COLUMN
        )
        val allColumnsToSelect = colsToSelect ++ Seq(AMOUNT_DEBIT_COLUMN, AMOUNT_CREDIT_COLUMN)
        val positiveAmount = transactionsDf
            .selectExpr((colsToSelect :+ s"$INVOICE_AMOUNT_COLUMN as $AMOUNT_DEBIT_COLUMN"):_*)
            .filter(s"$AMOUNT_DEBIT_COLUMN >= 0")
            .withColumn(AMOUNT_CREDIT_COLUMN, lit(null))
            .select(allColumnsToSelect.map(name => col(name)): _*)
        val negativeAmount = transactionsDf
            .selectExpr((colsToSelect :+ s"$INVOICE_AMOUNT_COLUMN as $AMOUNT_CREDIT_COLUMN"):_*)
            .filter(s"$AMOUNT_CREDIT_COLUMN < 0")
            .withColumn(AMOUNT_DEBIT_COLUMN, lit(null))
            .select(allColumnsToSelect.map(name => col(name)): _*)
        logger.info("Generated dataframe with a column for positive amount and another for negative amount!")
        positiveAmount.union(negativeAmount)
    }

    def unionAllRegionsData(mxDf: DataFrame, brDf: DataFrame, coDf: DataFrame, logger: GlueLogger): DataFrame = {
        logger.info("Generated one dataframe from union between all regions dataframes!")
        mxDf.union(brDf).union(coDf)
    }

    def mirrorTransactions(transactionsDf: DataFrame, logger: GlueLogger): DataFrame = {
        val colsToSelect = Seq(
            INSERT_DATE_COLUMN,
            UUID_COLUMN,
            DATE_COLUMN,
            ACCOUNTING_DATE_COLUMN,
            INVOICE_COLUMN,
            INVOICE_DESCRIPTION_COLUMN,
            TYPE_COLUMN,
            ID_CLIENT_COLUMN,
            s"$AMOUNT_CREDIT_COLUMN as $AMOUNT_DEBIT_COLUMN",
            s"$AMOUNT_DEBIT_COLUMN as $AMOUNT_CREDIT_COLUMN",
            YEAR_COLUMN,
            MONTH_COLUMN,
            DAY_COLUMN
        )
        val mirrorTransactions = transactionsDf
            .selectExpr(colsToSelect: _*)
        logger.info("Generated transactions with mirrored values!")
        transactionsDf.union(mirrorTransactions)
    }

    def getTransactionsWithNormalAccount(transactionsDf: DataFrame, logger: GlueLogger): DataFrame = {
        val colsToSelect = Seq(
            INSERT_DATE_COLUMN,
            UUID_COLUMN,
            DATE_COLUMN,
            ACCOUNTING_DATE_COLUMN,
            INVOICE_COLUMN,
            INVOICE_DESCRIPTION_COLUMN,
            TYPE_COLUMN,
            ID_CLIENT_COLUMN,
            AMOUNT_DEBIT_COLUMN,
            AMOUNT_CREDIT_COLUMN,
            NEW_NORMAL_CORRECTORA_COLUMN
        )
        val positiveDebit = transactionsDf
            .selectExpr((colsToSelect ++ Seq(DEBIT_ACCOUNT_COLUMN, YEAR_COLUMN, MONTH_COLUMN, DAY_COLUMN)):_*)
            .filter(s"$AMOUNT_DEBIT_COLUMN >= 0 and $NEW_NORMAL_CORRECTORA_COLUMN == 'Normal'")
            .withColumn(ACCOUNT_ID_COLUMN, col(DEBIT_ACCOUNT_COLUMN))
            .selectExpr((colsToSelect ++ Seq(ACCOUNT_ID_COLUMN, YEAR_COLUMN, MONTH_COLUMN, DAY_COLUMN)):_*)
        val positiveCredit = transactionsDf
            .selectExpr((colsToSelect ++ Seq(CREDIT_ACCOUNT_COLUMN, YEAR_COLUMN, MONTH_COLUMN, DAY_COLUMN)):_*)
            .filter(s"$AMOUNT_CREDIT_COLUMN >= 0 and $NEW_NORMAL_CORRECTORA_COLUMN == 'Normal'")
            .withColumn(ACCOUNT_ID_COLUMN, col(CREDIT_ACCOUNT_COLUMN))
            .selectExpr((colsToSelect ++ Seq(ACCOUNT_ID_COLUMN, YEAR_COLUMN, MONTH_COLUMN, DAY_COLUMN)):_*)
        logger.info("Generated transactions with account ID for Normal!")
        positiveDebit.union(positiveCredit)
    }

    def getTransactionsWithCorrectoraAccount(transactionsDf: DataFrame, logger: GlueLogger): DataFrame = {
        val colsToSelect = Seq(
            INSERT_DATE_COLUMN,
            UUID_COLUMN,
            DATE_COLUMN,
            ACCOUNTING_DATE_COLUMN,
            INVOICE_COLUMN,
            INVOICE_DESCRIPTION_COLUMN,
            TYPE_COLUMN,
            ID_CLIENT_COLUMN,
            AMOUNT_DEBIT_COLUMN,
            AMOUNT_CREDIT_COLUMN,
            NEW_NORMAL_CORRECTORA_COLUMN
        )
        val negativeDebit = transactionsDf
            .selectExpr((colsToSelect ++ Seq(DEBIT_ACCOUNT_COLUMN, YEAR_COLUMN, MONTH_COLUMN, DAY_COLUMN)):_*)
            .filter(s"$AMOUNT_DEBIT_COLUMN < 0 and $NEW_NORMAL_CORRECTORA_COLUMN == 'Correctora'")
            .withColumn(ACCOUNT_ID_COLUMN, col(DEBIT_ACCOUNT_COLUMN))
            .selectExpr((colsToSelect ++ Seq(ACCOUNT_ID_COLUMN, YEAR_COLUMN, MONTH_COLUMN, DAY_COLUMN)):_*)
        val negativeCredit = transactionsDf
            .selectExpr((colsToSelect ++ Seq(CREDIT_ACCOUNT_COLUMN, YEAR_COLUMN, MONTH_COLUMN, DAY_COLUMN)):_*)
            .filter(s"$AMOUNT_CREDIT_COLUMN < 0 and $NEW_NORMAL_CORRECTORA_COLUMN == 'Correctora'")
            .withColumn(ACCOUNT_ID_COLUMN, col(CREDIT_ACCOUNT_COLUMN))
            .selectExpr((colsToSelect ++ Seq(ACCOUNT_ID_COLUMN, YEAR_COLUMN, MONTH_COLUMN, DAY_COLUMN)):_*)
        logger.info("Generated transactions with account ID for Correctora!")
        negativeDebit.union(negativeCredit)
    }

    def getTransactionsWithAbsoluteValue(transactionsDf: DataFrame, logger: GlueLogger): DataFrame = {
        val colsToSelect = Seq(
            INSERT_DATE_COLUMN,
            UUID_COLUMN,
            DATE_COLUMN,
            ACCOUNTING_DATE_COLUMN,
            INVOICE_COLUMN,
            INVOICE_DESCRIPTION_COLUMN,
            TYPE_COLUMN,
            ID_CLIENT_COLUMN,
            NEW_NORMAL_CORRECTORA_COLUMN,
            s"abs($AMOUNT_DEBIT_COLUMN) as $AMOUNT_DEBIT_COLUMN",
            s"abs($AMOUNT_CREDIT_COLUMN) as $AMOUNT_CREDIT_COLUMN",
            ACCOUNT_ID_COLUMN,
            YEAR_COLUMN,
            MONTH_COLUMN,
            DAY_COLUMN
        )
        logger.info("Generated transactions with absolute value!")
        transactionsDf.selectExpr(colsToSelect: _*)
    }

    def getTransactionsIgnoringNaValues(transactionsDf: DataFrame, logger: GlueLogger): DataFrame = {
        val naValues = Seq("NA", "NAD", "NAC")
        logger.info("Remove all NA/NAD/NAC values!")
        transactionsDf.filter(!col(ACCOUNT_ID_COLUMN).isin(naValues  : _*)).na.drop(Seq(ACCOUNT_ID_COLUMN))
    }
}
