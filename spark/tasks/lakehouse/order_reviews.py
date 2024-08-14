from pydeequ.checks import *
from pydeequ.profiles import *
from pydeequ.verification import *
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql import DataFrame

from worker import LakehouseWorker
from exceptions import SparkError, SparkSkip

__all__ = ["LakehouseOrderReviewsWorker"]


class LakehouseOrderReviewsWorker(LakehouseWorker):
    def get_dataframe(self, force: bool = False, clean: bool = True, **kwargs) -> DataFrame:
        if self.dataframe is None and not force:
            spark = self.get_spark()
            object_name = self.get_object_name()

            schema = (
                "order_id string,"
                "review_id string,"
                "review_answer_timestamp timestamp,"
                "review_comment_message string,"
                "review_comment_title string,"
                "review_creation_date timestamp,"
                "review_score int"
            )

            df = (
                spark.read
                .format("csv")
                .schema(schema)
                .option("header", "true")
                .load(object_name)
            )

            if clean:
                df = df.filter(
                    "order_id is not null and "
                    "review_id is not null and "
                    "review_score is not null and "
                    "review_creation_date is not null and "
                    "length(review_id) = 32"  # uuid
                )

            self.dataframe = df

        return self.dataframe

    def test(self):
        spark = self.get_spark()
        df = self.get_dataframe()
        repository, result_key = self.get_pydeequ_repository(task="checks")
        check = Check(spark, CheckLevel.Error, "order_reviews")

        check_results = (
            VerificationSuite(spark)
            .onData(df)
            .addCheck(
                check
                .isComplete("order_id")
                .isComplete("review_id")
                .isComplete("review_creation_date")
                .hasDataType("review_score", ConstrainableDataTypes.Integral)
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

        df = df.withColumn("year", year(df["review_creation_date"])) \
            .withColumn("month", month(df["review_creation_date"])) \
            .withColumn("day", dayofmonth(df["review_creation_date"]))

        spark.sql("""
            CREATE TABLE IF NOT EXISTS iceberg.ecommerce.order_reviews (
                order_id string NOT NULL,
                review_id string NOT NULL,
                review_comment_title string,
                review_comment_message string,
                review_answer_timestamp timestamp_ntz,
                review_creation_date timestamp_ntz,
                review_score integer,
                year integer,
                month integer,
                day integer
            )
            USING iceberg
            PARTITIONED BY (year, month, day)
            TBLPROPERTIES (
                'write.format.default' = 'parquet',
                'write.parquet.compression-codec' = 'snappy'
            )
        """)

        df.createOrReplaceTempView("order_reviews")

        spark.sql("""
            MERGE INTO iceberg.ecommerce.order_reviews AS target
            USING order_reviews AS source
            ON 
                target.review_id = source.review_id AND
                target.order_id = source.order_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    def profile(self):
        spark = self.get_spark()
        df = spark.sql("select * from ecommerce.iceberg.order_reviews")
        repository, result_key = self.get_pydeequ_repository(task="profiles")

        (ColumnProfilerRunner(spark)
         .onData(df)
         .useRepository(repository)
         .saveOrAppendResult(result_key)
         .run())

        print(f"Profiles: {repository.path}")
