from dagster import (
    asset,
    AssetKey,
    MetadataValue,
    MaterializeResult,
    AssetExecutionContext,
)

from ecommerce.resources import DatabaseResource, IcebergResource


@asset(
    deps=[AssetKey("database_customers")],
    compute_kind="PySpark",
)
def lakehouse_customers(context: AssetExecutionContext, database: DatabaseResource, iceberg: IcebergResource):
    spark = iceberg.get_spark(app_name="customers")

    df = (
        spark.read.format("jdbc")
        .option("user", database.username)
        .option("password", database.password)
        .option("url", database.get_jdbc_connection())
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "customers")
        .load()
    )

    if not spark.catalog.tableExists("iceberg.ecommerce.customers"):
        res = (
            df.writeTo("iceberg.ecommerce.customers").using("iceberg")
            .option("write.parquet.compression-codec", "snappy")
            .create()
        )

    yield MaterializeResult(metadata={
        "count": MetadataValue.int(df.count()),
    })
