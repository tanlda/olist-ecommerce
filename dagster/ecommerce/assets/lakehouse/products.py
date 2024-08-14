from dagster import (
    asset,
    AssetKey,
    MetadataValue,
    MaterializeResult,
    AssetExecutionContext,
)

from ecommerce.resources import DatabaseResource, IcebergResource


@asset(
    deps=[AssetKey("database_products")],
    compute_kind="PySpark",
)
def lakehouse_products(context: AssetExecutionContext, database: DatabaseResource, iceberg: IcebergResource):
    spark = iceberg.get_spark(app_name="products")

    df = (
        spark.read.format("jdbc")
        .option("user", database.username)
        .option("password", database.password)
        .option("url", database.get_jdbc_connection())
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "products")
        .load()
    )

    if not spark.catalog.tableExists("iceberg.ecommerce.products"):
        res = (
            df.writeTo("iceberg.ecommerce.products").using("iceberg")
            .option("write.parquet.compression-codec", "snappy")
            .create()
        )

    yield MaterializeResult(metadata={
        "count": MetadataValue.int(df.count()),
    })
