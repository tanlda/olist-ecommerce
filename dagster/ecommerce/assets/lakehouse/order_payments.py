from pydeequ.checks import *
from pydeequ.verification import *
from pyspark.sql.functions import lit
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
    deps=[AssetKey("database_order_payments")],
    partitions_def=daily_partitions_def,
    check_specs=[AssetCheckSpec(name="lakehouse_order_payments_check", asset="lakehouse_order_payments")],
    compute_kind="PySpark",
)
def lakehouse_order_payments(
        context: AssetExecutionContext,
        database: DatabaseResource,
        iceberg: IcebergResource,
        pydeequ: PyDeequResource,
):
    partition = context.partition_key
    year, month, day = partition.split("-")

    spark = iceberg.get_spark(app_name="orders-payments", connect_lakehouse=True)

    query = rf"""
        (WITH selected AS (
            SELECT order_id FROM orders
            WHERE date_trunc('day', order_purchase_timestamp) = date '{partition}'
        )
        SELECT payments.*
        FROM selected
        LEFT JOIN order_payments payments
        ON payments.order_id = selected.order_id) AS order_payments
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
        df = (
            df
            .withColumn("year", lit(year))
            .withColumn("month", lit(month))
            .withColumn("day", lit(day))
        )

        if not spark.catalog.tableExists("iceberg.ecommerce.order_payments"):
            res = (
                df.writeTo("iceberg.ecommerce.order_payments")
                .using("iceberg")
                .partitionedBy("year", "month", "day")
                .option("write.parquet.compression-codec", "snappy")
                .create()
            )

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

    check = Check(spark, CheckLevel.Error, "order_payments")
    repository, result_key = pydeequ.get_repository(spark, table="order_payments", partition=partition, task="checks")

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
    context.log.info(f"Verification: {verification_df}")
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
