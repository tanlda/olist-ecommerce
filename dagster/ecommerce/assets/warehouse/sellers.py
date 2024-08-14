from dagster import (
    asset,
    AssetKey,
    MetadataValue,
    MaterializeResult,
    AssetExecutionContext,
)

from ecommerce.resources import IcebergResource, WarehouseResource


@asset(
    deps=[AssetKey("lakehouse_sellers")],
    compute_kind="PySpark",
)
def warehouse_sellers(
        context: AssetExecutionContext,
        warehouse: WarehouseResource,
        iceberg: IcebergResource,
):
    url, properties = warehouse.get_jdbc()
    spark = iceberg.get_spark(app_name="sellers")

    df = spark.sql("SELECT * FROM iceberg.ecommerce.sellers")

    if not df.isEmpty():
        df.write.jdbc(
            url=url,
            mode="overwrite",
            table="sellers",
            properties=properties,
        )

    yield MaterializeResult(metadata={
        "count": MetadataValue.int(df.count())
    })
