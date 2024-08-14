from dagster import (
    asset,
    AssetKey,
    MetadataValue,
    MaterializeResult,
    AssetExecutionContext,
)

from ecommerce.resources import IcebergResource, WarehouseResource


@asset(
    deps=[AssetKey("lakehouse_customers")],
    compute_kind="PySpark",
)
def warehouse_customers(
        context: AssetExecutionContext,
        warehouse: WarehouseResource,
        iceberg: IcebergResource,
):
    url, properties = warehouse.get_jdbc()
    spark = iceberg.get_spark(app_name="customers")

    df = spark.sql("SELECT * FROM iceberg.ecommerce.customers")

    if not df.isEmpty():
        df.write.jdbc(
            url=url,
            mode="overwrite",
            table="customers",
            properties=properties,
        )

    yield MaterializeResult(metadata={
        "count": MetadataValue.int(df.count())
    })
