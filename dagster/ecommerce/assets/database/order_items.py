import asyncio
import polars as pl

from dagster import (
    asset,
    AssetKey,
    MetadataValue,
    MaterializeResult,
    AssetExecutionContext,
)

from ecommerce.utils import calculate_ttl_from
from ecommerce.resources import DatabaseResource, StorageResource, GlobalResource
from ecommerce.partitions import monthly_partitions_def


@asset(
    deps=[AssetKey("storage_ecommerce")],
    partitions_def=monthly_partitions_def,
    compute_kind="Polars",
)
async def database_order_items(
        context: AssetExecutionContext,
        storage: StorageResource,
        database: DatabaseResource,
        global_: GlobalResource,
):
    year, month = context.partition_key.split("-")

    orders = storage.read_csv("raw/orders.csv")
    order_items = storage.read_csv("raw/order_items.csv")
    order_item_events = (
        order_items
        .join(orders, on="order_id", how="left")
        .with_columns(
            pl.col("order_purchase_timestamp").str.to_datetime(),
            pl.col("shipping_limit_date").str.to_datetime(),
        )
        .filter(
            pl.col("order_purchase_timestamp").dt.year() == int(year),
            pl.col("order_purchase_timestamp").dt.month() == int(month),
        )
        .drop_nulls(["order_purchase_timestamp"])
    )

    columns = [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value"
    ]

    order_item_events = calculate_ttl_from(order_item_events.select(
        *columns,
        timestamp="order_purchase_timestamp",
        purchased_timestamp="order_purchase_timestamp",
    ))

    context.log.info(order_item_events.describe())
    conn = database.get_psycopg(autocommit=True)
    cursor = conn.cursor()

    if global_.delete:
        cursor.execute("DELETE FROM order_items;")

    if global_.drop:
        cursor.execute("DROP TABLE IF EXISTS order_items;")

    async def process(event):
        await asyncio.sleep(event["ttl"])
        df = pl.DataFrame(event).select(*columns)
        df.write_database(
            table_name="order_items",
            connection=database.get_connection(),
            if_table_exists="append",
            engine="sqlalchemy",
        )

    async def main():
        tasks = [process(event) for event in order_item_events.to_dicts()]
        await asyncio.gather(*tasks)
        cursor.close()
        conn.close()

    await main()

    yield MaterializeResult(metadata={
        "len": MetadataValue.int(len(order_item_events))
    })
