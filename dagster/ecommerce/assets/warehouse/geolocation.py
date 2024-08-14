from dagster import (
    asset,
    AssetKey,
    MetadataValue,
    MaterializeResult,
    AssetExecutionContext,
)

from ecommerce.resources import IcebergResource, WarehouseResource


@asset(
    deps=[AssetKey("lakehouse_geolocation")],
    compute_kind="PySpark",
)
def warehouse_geolocation(
        context: AssetExecutionContext,
        warehouse: WarehouseResource,
        iceberg: IcebergResource,
):
    url, properties = warehouse.get_jdbc()
    spark = iceberg.get_spark(app_name="geolocation")

    df = spark.sql("SELECT * FROM iceberg.ecommerce.geolocation")

    if not df.isEmpty():
        df.write.jdbc(
            url=url,
            mode="overwrite",
            table="geolocation",
            properties=properties,
        )

    yield MaterializeResult(metadata={
        "count": MetadataValue.int(df.count())
    })
