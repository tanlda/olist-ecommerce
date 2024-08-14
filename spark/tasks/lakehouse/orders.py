from pydeequ.checks import *
from pydeequ.profiles import *
from pydeequ.verification import *
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql import DataFrame

from worker import LakehouseWorker
from exceptions import SparkError, SparkSkip

__all__ = ["LakehouseOrdersWorker"]


class LakehouseOrdersWorker(LakehouseWorker):
    def get_dataframe(self, force: bool = False, clean: bool = True, **kwargs) -> DataFrame:
        if self.dataframe is None and not force:
            schema = (
                "order_id string,"
                "customer_id string,"
                "order_status string,"
                "order_purchase_timestamp timestamp,"
                "order_approved_at timestamp,"
                "order_delivered_carrier_date timestamp,"
                "order_delivered_customer_date timestamp,"
                "order_estimated_delivery_date timestamp"
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
        check = Check(spark, CheckLevel.Error, "orders")

        check_results = (
            VerificationSuite(spark)
            .onData(df)
            .addCheck(
                check
                .isUnique("order_id")
                .isComplete("order_id")
                .isComplete("customer_id")
                .isComplete("order_purchase_timestamp")
                .isComplete("order_estimated_delivery_date")
                .isContainedIn("order_status", [
                    "approved",
                    "processing",
                    "invoiced",
                    "canceled",
                    "delivered",
                    "shipped",
                    "unavailable",
                    "created"
                ])
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

        if df.isEmpty():
            raise SparkSkip()

        df = df.withColumn("year", year(df["order_purchase_timestamp"])) \
            .withColumn("month", month(df["order_purchase_timestamp"])) \
            .withColumn("day", dayofmonth(df["order_purchase_timestamp"]))

        if not spark.catalog.tableExists("iceberg.ecommerce.orders"):
            df.writeTo("iceberg.ecommerce.orders") \
                .using("iceberg") \
                .partitionedBy("year", "month", "day") \
                .option("write.parquet.compression-codec", "snappy") \
                .create()

        df.createOrReplaceTempView("orders")

        spark.sql("""
            MERGE INTO iceberg.ecommerce.orders AS target
            USING orders AS source
            ON target.order_id = source.order_id
            WHEN MATCHED THEN UPDATE SET
                target.order_status = source.order_status,
                target.order_approved_at = source.order_approved_at,
                target.order_delivered_carrier_date = source.order_delivered_carrier_date,
                target.order_delivered_customer_date = source.order_delivered_customer_date
            WHEN NOT MATCHED THEN INSERT *
        """)

    def profile(self):
        spark = self.get_spark()
        df = spark.sql("select * from ecommerce.iceberg.orders")
        repository, result_key = self.get_pydeequ_repository(task="profiles")

        (ColumnProfilerRunner(spark)
         .onData(df)
         .useRepository(repository)
         .saveOrAppendResult(result_key)
         .run())

        print(f"Profiles: {repository.path}")
