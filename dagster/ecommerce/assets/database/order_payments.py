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
async def database_order_payments(
        context: AssetExecutionContext,
        database: DatabaseResource,
        storage: StorageResource,
        global_: GlobalResource,
):
    year, month = context.partition_key.split("-")

    orders = storage.read_csv("raw/orders.csv")
    order_payments = storage.read_csv("raw/order_payments.csv")

    order_payment_events = (
        order_payments
        .join(orders, on="order_id", how="left")
        .with_columns(
            pl.col("order_purchase_timestamp").str.to_datetime(),
            pl.col("order_approved_at").str.to_datetime(),
        )
        .filter(
            pl.col("order_purchase_timestamp").dt.year() == int(year),
            pl.col("order_purchase_timestamp").dt.month() == int(month),
        )
        .drop_nulls(["order_purchase_timestamp"])
    )

    columns = [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    ]

    approved_events = calculate_ttl_from(order_payment_events.select(
        *columns,
        timestamp="order_approved_at",
        purchased_timestamp="order_purchase_timestamp",
    ))

    context.log.info(approved_events.describe())
    conn = database.get_psycopg(autocommit=True)
    cursor = conn.cursor()

    if global_.delete:
        cursor.execute("DELETE FROM order_payments;")

    if global_.drop:
        cursor.execute("DROP TABLE IF EXISTS order_payments;")

    async def process(event: dict):
        await asyncio.sleep(event["ttl"])
        df = pl.DataFrame(event).select(*columns)
        df.write_database(
            table_name="order_payments",
            connection=database.get_connection(),
            if_table_exists="append",
            engine="sqlalchemy",
        )

    async def main():
        tasks = [process(event) for event in approved_events.to_dicts()]
        await asyncio.gather(*tasks)
        cursor.close()
        conn.close()

    await main()

    yield MaterializeResult(metadata={
        "len": MetadataValue.int(len(approved_events))
    })
