from pydeequ.checks import *
from pydeequ.verification import *
from pyspark.sql.functions import year, month, dayofmonth
from dagster import (
    asset,
    AssetKey,
    MetadataValue,
    MaterializeResult,
    AssetExecutionContext,
    AssetCheckSeverity,
    AssetCheckResult,
    AssetCheckSpec,
)

from ecommerce.partitions import daily_partitions_def
from ecommerce.resources import DatabaseResource, IcebergResource, PyDeequResource


@asset(
    deps=[AssetKey("document_order_reviews")],
    partitions_def=daily_partitions_def,
    check_specs=[AssetCheckSpec(name="lakehouse_order_reviews_check", asset="lakehouse_order_reviews")],
    compute_kind="PySpark",
)
def lakehouse_order_reviews(
        context: AssetExecutionContext,
        database: DatabaseResource,
        iceberg: IcebergResource,
        pydeequ: PyDeequResource,
):
    partition = context.partition_key
    spark = iceberg.get_spark(app_name="order-reviews", connect_document=True, connect_lakehouse=True)

    df = (
        spark.read
        .format("mongodb")
        .option("database", "ecommerce")
        .option("collection", "order_reviews")
        .load()
    )

    df.createOrReplaceTempView("order_reviews")

    df = spark.sql(rf"""
        SELECT * FROM order_reviews
        WHERE date_trunc('day', review_creation_date) = date '{partition}' 
    """)

    if not df.isEmpty():
        timestamp_col = "review_creation_date"

        df = (
            df
            .withColumn("year", year(df[timestamp_col]))
            .withColumn("month", month(df[timestamp_col]))
            .withColumn("day", dayofmonth(df[timestamp_col]))
        )

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

    check = Check(spark, CheckLevel.Error, "order_reviews")
    repository, result_key = pydeequ.get_repository(spark, table="order_reviews", partition=partition, task="checks")

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
    context.log.info(f"Checks: {repository.path}")

    if has_error and not df.isEmpty():
        yield AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Asset check failed. Checks result: {repository.path}",
        )
    else:
        yield AssetCheckResult(
            passed=True,
            description="Asset check success",
        )

    spark.sparkContext._gateway.shutdown_callback_server()  # noqa
    yield MaterializeResult(metadata={
        "date": MetadataValue.text(partition),
        "count": MetadataValue.int(df.count()),
    })
