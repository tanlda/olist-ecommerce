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
from ecommerce.assets.warehouse.constants import merge_query_tpl


@asset(
    deps=[AssetKey("lakehouse_order_payments")],
    partitions_def=daily_partitions_def,
    automation_condition=AutomationCondition.eager(),
    compute_kind="PySpark",
)
def warehouse_order_payments(
        context: AssetExecutionContext,
        warehouse: WarehouseResource,
        iceberg: IcebergResource,
):
    url, properties = warehouse.get_jdbc()
    spark = iceberg.get_spark(app_name="order_payments")
    year, month, day = context.partition_key.split("-")

    df = spark.createDataFrame([], "")
    if spark.catalog.tableExists("iceberg.ecommerce.order_payments"):
        df = spark.sql(rf"""
            SELECT * FROM iceberg.ecommerce.order_payments
            WHERE year = '{year}' AND month = '{month}' AND day = '{day}'
        """)

    staging_table = ""
    if not df.isEmpty():
        df.write.jdbc(
            url=url,
            mode="ignore",
            table="order_payments",
            properties=properties,
        )

        staging_table = f"order_payments_staging_{uuid.uuid4().hex}"

        df.write.jdbc(
            url=url,
            mode="overwrite",
            table=staging_table,
            properties=properties,
        )

        conn = warehouse.get_psycopg(autocommit=True)

        with conn.cursor() as cursor:
            merge_query = merge_query_tpl.format(
                where="table_name = 'order_payments' AND column_name != 'order_id';",
                template=f"""
                MERGE INTO order_payments t
                USING {staging_table} s
                ON 
                    t.order_id = s.order_id AND
                    t.payment_type = s.payment_type AND
                    t.payment_sequential = s.payment_sequential AND
                    t.payment_installments = s.payment_installments
                WHEN MATCHED THEN
                    UPDATE SET %s
                WHEN NOT MATCHED THEN
                    INSERT (order_id, %s)
                    VALUES (s.order_id, %s)
            """
            )

            cursor.execute(merge_query)

            drop_query = f"DROP TABLE IF EXISTS {staging_table};"
            cursor.execute(drop_query)

        conn.close()

    yield MaterializeResult(metadata={
        "count": MetadataValue.int(df.count()),
        "staging_table": MetadataValue.text(staging_table),
    })
