from pydeequ.checks import *
from pydeequ.profiles import *
from pydeequ.verification import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from worker import LakehouseWorker
from exceptions import SparkError, SparkSkip

__all__ = ["LakehouseOrderPaymentsWorker"]


class LakehouseOrderPaymentsWorker(LakehouseWorker):
    dataframe: DataFrame = None

    def get_dataframe(self, force: bool = False, clean: bool = True, **kwargs) -> DataFrame:
        if self.dataframe is None and not force:
            schema = (
                "order_id string,"
                "payment_sequential int,"
                "payment_type string,"
                "payment_installments int,"
                "payment_value float"
            )
            spark = self.get_spark()
            object_name = self.get_object_name()
            self.dataframe = (
                spark.read
                .format("csv")
                .schema(schema)
                .option("header", "true")
                .load(object_name)
            )

        return self.dataframe

    def test(self):
        spark = self.get_spark()
        df = self.get_dataframe()
        repository, result_key = self.get_pydeequ_repository(task="checks")
        check = Check(spark, CheckLevel.Error, "order_payments")

        check_results = (
            VerificationSuite(spark)
            .onData(df)
            .addCheck(
                check
                .isComplete("order_id")
                .hasDataType("payment_value", ConstrainableDataTypes.Fractional)
                .hasDataType("payment_sequential", ConstrainableDataTypes.Integral)
                .hasDataType("payment_installments", ConstrainableDataTypes.Integral)
                .isContainedIn("payment_type", ["credit_card", "debit_card", "voucher", "boleto", "not_defined"])
            )
            .useRepository(repository)
            .saveOrAppendResult(result_key)
            .run()
        )

        verification_df = VerificationResult.checkResultsAsDataFrame(spark, check_results)
        has_error = verification_df.filter(verification_df.check_status == "Error").count()
        print(f"Checks: {repository.path}")

        if has_error and not df.isEmpty():
            raise SparkError()

    def ingest(self):
        spark = self.get_spark()
        df = self.get_dataframe()
        year, month, day = self.args.date_cursor.split("-")

        if df.isEmpty():
            raise SparkSkip()

        df = (
            df
            .withColumn("year", lit(year))
            .withColumn("month", lit(month))
            .withColumn("day", lit(day))
        )

        if not spark.catalog.tableExists("iceberg.ecommerce.order_payments"):
            (df
             .writeTo("iceberg.ecommerce.order_payments")
             .using("iceberg")
             .partitionedBy("year", "month", "day")
             .option("write.parquet.compression-codec", "snappy")
             .create())

        df.createOrReplaceTempView("order_payments")

        spark.sql("""
            MERGE INTO iceberg.ecommerce.order_payments AS target
            USING order_payments AS source
            ON target.order_id = source.order_id
                AND target.payment_sequential = source.payment_sequential
                AND target.payment_installments = source.payment_installments
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    def profile(self):
        spark = self.get_spark()
        df = spark.sql("select * from ecommerce.iceberg.order_payments")
        repository, result_key = self.get_pydeequ_repository(task="profiles")

        (ColumnProfilerRunner(spark)
         .onData(df)
         .useRepository(repository)
         .saveOrAppendResult(result_key)
         .run())

        print(f"Profiles: {repository.path}")
