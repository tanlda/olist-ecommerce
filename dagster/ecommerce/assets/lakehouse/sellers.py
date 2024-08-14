from dagster import (
    asset,
    AssetKey,
    MetadataValue,
    MaterializeResult,
    AssetExecutionContext,
)

from ecommerce.resources import DatabaseResource, IcebergResource


@asset(
    deps=[AssetKey("database_sellers")],
    compute_kind="PySpark",
)
def lakehouse_sellers(context: AssetExecutionContext, database: DatabaseResource, iceberg: IcebergResource):
    spark = iceberg.get_spark(app_name="sellers")

    df = (
        spark.read.format("jdbc")
        .option("user", database.username)
        .option("password", database.password)
        .option("url", database.get_jdbc_connection())
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "sellers")
        .load()
    )

    if not spark.catalog.tableExists("iceberg.ecommerce.sellers"):
        res = (
            df.writeTo("iceberg.ecommerce.sellers").using("iceberg")
            .option("write.parquet.compression-codec", "snappy")
            .create()
        )

    yield MaterializeResult(metadata={
        "count": MetadataValue.int(df.count()),
    })
