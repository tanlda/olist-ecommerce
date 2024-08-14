from pydeequ.checks import *
from pydeequ.profiles import *
from pydeequ.verification import *
from pyspark.sql import DataFrame

from worker import LakehouseWorker
from exceptions import SparkError, SparkSkip

__all__ = ["LakehouseOrderItemsWorker"]


class LakehouseOrderItemsWorker(LakehouseWorker):
    def get_dataframe(self, force: bool = False, clean: bool = True, **kwargs) -> DataFrame:
        if self.dataframe is None and not force:
            schema = (
                "order_id string,"
                "order_item_id int,"
                "product_id string,"
                "seller_id string,"
                "shipping_limit_date timestamp,"
                "price float,"
                "freight_value float"
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
        check = Check(spark, CheckLevel.Error, "order_items")

        check_results = (
            VerificationSuite(spark)
            .onData(df)
            .addCheck(
                check
                .isComplete("order_id")
                .isComplete("seller_id")
                .isComplete("product_id")

                .isComplete("price")
                .isPositive("price")
                .hasDataType("price", ConstrainableDataTypes.Numeric)

                .isComplete("freight_value")
                .isPositive("freight_value")
                .hasDataType("freight_value", ConstrainableDataTypes.Numeric)

                .isComplete("order_item_id")
                .isPositive("order_item_id")
                .hasDataType("order_item_id", ConstrainableDataTypes.Integral)
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

        if not spark.catalog.tableExists("iceberg.ecommerce.order_items"):
            (df
             .writeTo("iceberg.ecommerce.order_items").using("iceberg")
             .option("write.parquet.compression-codec", "snappy")
             .create())

        df.createOrReplaceTempView("order_items")

        spark.sql("""
            MERGE INTO iceberg.ecommerce.order_items AS target
            USING order_items AS source
            ON 
                target.order_id = source.order_id AND
                target.product_id = source.product_id AND
                target.seller_id = source.seller_id AND
                target.order_item_id = source.order_item_id AND
                target.shipping_limit_date = source.shipping_limit_date
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    def profile(self):
        spark = self.get_spark()
        df = spark.sql("select * from ecommerce.iceberg.order_items")
        repository, result_key = self.get_pydeequ_repository(task="profiles")

        (ColumnProfilerRunner(spark)
         .onData(df)
         .useRepository(repository)
         .saveOrAppendResult(result_key)
         .run())

        print(f"Profiles: {repository.path}")
