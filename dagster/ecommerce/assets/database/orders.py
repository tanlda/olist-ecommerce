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
async def database_orders(
        context: AssetExecutionContext,
        storage: StorageResource,
        database: DatabaseResource,
        global_: GlobalResource,
):
    year, month = context.partition_key.split("-")
    orders = storage.read_csv("raw/orders.csv")
    order_events = (
        orders
        .with_columns(
            pl.col("order_purchase_timestamp").str.to_datetime(),
            pl.col("order_approved_at").str.to_datetime(),
            pl.col("order_delivered_carrier_date").str.to_datetime(),
            pl.col("order_delivered_customer_date").str.to_datetime(),
            pl.col("order_estimated_delivery_date").str.to_datetime(),
        )
        .filter(
            pl.col("order_purchase_timestamp").dt.year() == int(year),
            pl.col("order_purchase_timestamp").dt.month() == int(month),
        )
        .drop_nulls(["order_purchase_timestamp"])
    )

    purchased_events = calculate_ttl_from(order_events.select(
        "order_id",
        "customer_id",
        "order_estimated_delivery_date",
        timestamp="order_purchase_timestamp",
        purchased_timestamp="order_purchase_timestamp",
    ))

    approved_events = calculate_ttl_from(order_events.select(
        "order_id",
        timestamp="order_approved_at",
        purchased_timestamp="order_purchase_timestamp",
    ))

    delivered_carrier_events = calculate_ttl_from(order_events.select(
        "order_id",
        timestamp="order_delivered_carrier_date",
        purchased_timestamp="order_purchase_timestamp",
    ))

    delivered_customer_events = calculate_ttl_from(order_events.select(
        "order_id",
        timestamp="order_delivered_customer_date",
        purchased_timestamp="order_purchase_timestamp",
    ))

    estimated_delivery_events = calculate_ttl_from(order_events.select(
        "order_id",
        "order_status",
        timestamp="order_estimated_delivery_date",
        purchased_timestamp="order_purchase_timestamp",
    ))

    context.log.info(purchased_events.describe())
    context.log.info(estimated_delivery_events.describe())
    conn = database.get_psycopg(autocommit=True)
    cursor = conn.cursor()

    if global_.delete:
        cursor.execute("DELETE FROM orders;")

    if global_.drop:
        cursor.execute("DROP TABLE IF EXISTS orders;")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id VARCHAR(255) UNIQUE,
        customer_id VARCHAR(255),
        order_status VARCHAR(50),
        order_purchase_timestamp TIMESTAMP NOT NULL,
        order_approved_at TIMESTAMP,
        order_delivered_carrier_date TIMESTAMP,
        order_delivered_customer_date TIMESTAMP,
        order_estimated_delivery_date TIMESTAMP
    );
    """)

    async def process_purchased(event):
        await asyncio.sleep(event["ttl"])
        insert_query = """
        INSERT INTO orders (
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id)
        DO NOTHING;
        """
        insert_data = (
            event["order_id"],
            event["customer_id"],
            "created",
            event["timestamp"],
            None,
            None,
            None,
            event["order_estimated_delivery_date"],
        )

        cursor.execute(insert_query, insert_data)

    async def process_approved(event):
        await asyncio.sleep(event["ttl"])
        update_query = """
        UPDATE orders
        SET order_status = %s, order_approved_at = %s
        WHERE order_id = %s
        """
        update_data = ("approved", event["timestamp"], event["order_id"])
        cursor.execute(update_query, update_data)

    async def process_delivered_carrier(event):
        await asyncio.sleep(event["ttl"])
        update_query = """
        UPDATE orders
        SET order_status = %s, order_delivered_carrier_date = %s
        WHERE order_id = %s
        """
        update_data = ("shipped", event["timestamp"], event["order_id"])
        cursor.execute(update_query, update_data)

    async def process_delivered_customer(event):
        await asyncio.sleep(event["ttl"])
        update_query = """
        UPDATE orders
        SET order_status = %s, order_delivered_customer_date = %s
        WHERE order_id = %s
        """
        update_data = ("delivered", event["timestamp"], event["order_id"])
        cursor.execute(update_query, update_data)

    async def process_estimated_delivery(event):
        await asyncio.sleep(event["ttl"])
        update_query = """
        UPDATE orders
        SET order_status = %s
        WHERE order_id = %s
        """
        update_data = (event["order_status"], event["order_id"])
        cursor.execute(update_query, update_data)

    async def main():
        purchased_tasks = [process_purchased(event) for event in purchased_events.to_dicts()]
        approved_tasks = [process_approved(event) for event in approved_events.to_dicts()]
        delivered_carrier_tasks = [process_delivered_carrier(event) for event in delivered_carrier_events.to_dicts()]
        delivered_customer_tasks = [process_delivered_customer(event) for event in delivered_customer_events.to_dicts()]
        estimated_delivery_tasks = [process_estimated_delivery(event) for event in estimated_delivery_events.to_dicts()]
        await asyncio.gather(
            *purchased_tasks,
            *approved_tasks,
            *delivered_carrier_tasks,
            *delivered_customer_tasks,
            *estimated_delivery_tasks,
        )
        cursor.close()
        conn.close()

    await main()

    yield MaterializeResult(metadata={
        "approved_events (len)": MetadataValue.int(len(approved_events)),
        "purchased_events (len)": MetadataValue.int(len(purchased_events)),
        "delivered_carrier_events (len)": MetadataValue.int(len(delivered_carrier_events)),
        "delivered_customer_events (len)": MetadataValue.int(len(delivered_customer_events)),
    })
