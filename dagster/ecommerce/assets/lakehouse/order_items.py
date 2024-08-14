from pydeequ.checks import *
from pydeequ.verification import *
from dagster import (
    asset,
    AssetKey,
    MetadataValue,
    MaterializeResult,
    AssetExecutionContext,
    AssetCheckSeverity,
    AssetCheckResult,
    AssetCheckSpec,
    Output,
)

from ecommerce.partitions import daily_partitions_def
from ecommerce.resources import DatabaseResource, IcebergResource, PyDeequResource


@asset(
    deps=[AssetKey("database_order_items")],
    partitions_def=daily_partitions_def,
    check_specs=[AssetCheckSpec(name="lakehouse_order_items_check", asset="lakehouse_order_items")],
    compute_kind="PySpark",
)
def lakehouse_order_items(
        context: AssetExecutionContext,
        database: DatabaseResource,
        iceberg: IcebergResource,
        pydeequ: PyDeequResource,
):
    partition = context.partition_key

    spark = iceberg.get_spark(app_name="orders-items", connect_lakehouse=True)

    query = rf"""
        (WITH selected AS (
            SELECT order_id FROM orders
            WHERE date_trunc('day', order_purchase_timestamp) = date '{partition}'
        )
        SELECT items.*
        FROM selected
        LEFT JOIN order_items items
        ON items.order_id = selected.order_id) AS order_items
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
        if not spark.catalog.tableExists("iceberg.ecommerce.order_items"):
            res = (
                df.writeTo("iceberg.ecommerce.order_items")
                .using("iceberg")
                .option("write.parquet.compression-codec", "snappy")
                .create()
            )

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

    check = Check(spark, CheckLevel.Error, "order_items")
    repository, result_key = pydeequ.get_repository(spark, table="order_items", partition=partition, task="checks")

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
