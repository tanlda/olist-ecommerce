import uuid
from dagster import (
    asset,
    AssetKey,
    MetadataValue,
    MaterializeResult,
    AssetExecutionContext,
    AutomationCondition,
)

from ecommerce.partitions import daily_partitions_def
from ecommerce.resources import IcebergResource, WarehouseResource


@asset(
    deps=[AssetKey("lakehouse_order_items")],
    partitions_def=daily_partitions_def,
    automation_condition=AutomationCondition.eager(),
    compute_kind="PySpark",
)
def warehouse_order_items(
        context: AssetExecutionContext,
        warehouse: WarehouseResource,
        iceberg: IcebergResource,
):
    url, properties = warehouse.get_jdbc()
    spark = iceberg.get_spark(app_name="order_items")
    year, month, day = context.partition_key.split("-")

    df = spark.createDataFrame([], "")
    if spark.catalog.tableExists("iceberg.ecommerce.order_items"):
        df = spark.sql(rf"""
            SELECT items.*
            FROM iceberg.ecommerce.orders orders
            LEFT JOIN iceberg.ecommerce.order_items items ON orders.order_id = items.order_id
            WHERE orders.year = '{year}' AND orders.month = '{month}' AND orders.day = '{day}'
        """)

    staging_table = ""
    if not df.isEmpty():
        df.write.jdbc(
            url=url,
            mode="ignore",
            table="order_items",
            properties=properties,
        )

        staging_table = f"order_items_staging_{uuid.uuid4().hex}"

        df.write.jdbc(
            url=url,
            mode="overwrite",
            table=staging_table,
            properties=properties,
        )

        conn = warehouse.get_psycopg(autocommit=True)

        with conn.cursor() as cursor:
            merge_query = f"""
                MERGE INTO order_items t
                USING {staging_table} s
                ON
                    t.order_id = s.order_id AND
                    t.product_id = s.product_id AND
                    t.seller_id = s.seller_id AND
                    t.order_item_id = s.order_item_id AND
                    t.shipping_limit_date = s.shipping_limit_date
                WHEN NOT MATCHED THEN
                    INSERT (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value)
                    VALUES (order_id, s.order_item_id, s.product_id, s.seller_id, s.shipping_limit_date, s.price, s.freight_value)
            """

            cursor.execute(merge_query)

            drop_query = f"DROP TABLE IF EXISTS {staging_table};"
            cursor.execute(drop_query)

        conn.close()

    yield MaterializeResult(metadata={
        "count": MetadataValue.int(df.count()),
        "staging_table": MetadataValue.text(staging_table),
    })
