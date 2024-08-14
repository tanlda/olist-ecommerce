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
    deps=[AssetKey("database_orders")],
    partitions_def=daily_partitions_def,
    check_specs=[AssetCheckSpec(name="lakehouse_orders_check", asset="lakehouse_orders")],
    compute_kind="PySpark",
)
def lakehouse_orders(
        context: AssetExecutionContext,
        database: DatabaseResource,
        iceberg: IcebergResource,
        pydeequ: PyDeequResource,
):
    partition = context.partition_key

    spark = iceberg.get_spark(app_name="orders", connect_lakehouse=True)

    query = rf"""
        (SELECT * FROM orders
        WHERE date_trunc('day', order_purchase_timestamp) = date '{partition}') AS orders 
    """

    df = (
        spark.read.format("jdbc")
        .option("user", database.username)
        .option("password", database.password)
        .option("url", database.get_jdbc_connection())
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", query)
        .load()
    )

    if not df.isEmpty():
        timestamp_col = "order_purchase_timestamp"

        df = (
            df
            .withColumn("year", year(df[timestamp_col]))
            .withColumn("month", month(df[timestamp_col]))
            .withColumn("day", dayofmonth(df[timestamp_col]))
        )

        if not spark.catalog.tableExists("iceberg.ecommerce.orders"):
            res = (
                df.writeTo("iceberg.ecommerce.orders")
                .using("iceberg")
                .partitionedBy("year", "month", "day")
                .option("write.parquet.compression-codec", "snappy")
                .create()
            )

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

    check = Check(spark, CheckLevel.Error, "orders")
    repository, result_key = pydeequ.get_repository(spark, table="orders", partition=partition, task="checks")

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
