from dagster import (
    asset,
    AssetKey,
    MetadataValue,
    MaterializeResult,
    AssetExecutionContext,
)

from ecommerce.resources import IcebergResource, WarehouseResource


@asset(
    deps=[AssetKey("lakehouse_products")],
    compute_kind="PySpark",
)
def warehouse_products(
        context: AssetExecutionContext,
        warehouse: WarehouseResource,
        iceberg: IcebergResource,
):
    url, properties = warehouse.get_jdbc()
    spark = iceberg.get_spark(app_name="products")

    df = spark.sql("SELECT * FROM iceberg.ecommerce.products")

    if not df.isEmpty():
        df.write.jdbc(
            url=url,
            mode="overwrite",
            table="products",
            properties=properties,
        )

    yield MaterializeResult(metadata={
        "count": MetadataValue.int(df.count())
    })
