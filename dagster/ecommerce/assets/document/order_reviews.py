import asyncio
import polars as pl
from dagster import (
    asset,
    AssetKey,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    Output,
)
from contextlib import suppress
from tempfile import NamedTemporaryFile
from huggingface_hub import InferenceClient

from ecommerce.utils import calculate_ttl_from
from ecommerce.resources import StorageResource, DocumentResource, GlobalResource
from ecommerce.partitions import monthly_partitions_def


@asset(
    deps=["storage_ecommerce"],
    partitions_def=monthly_partitions_def,
    compute_kind="PyTorch",
)
def storage_order_reviews(context: AssetExecutionContext, storage: StorageResource):
    """Translate all reviews from Portuguese to English"""
    minio = storage.get_client()
    partition = context.partition_key
    response = minio.get_object("ecommerce", "raw/order_reviews.csv")
    df = pl.read_csv(response.read())

    if minio:
        yield Output(None)

    df = (
        df
        .with_columns(pl.col("review_creation_date").str.to_datetime())
        .filter(pl.col("review_creation_date").dt.strftime("%Y-%m") == partition)
    )

    count = 0
    total = len(df)
    context.log.info(f"Partition: {partition} - Total: {total}")
    inference = InferenceClient(model="http://translate.ai")

    def translate(text: str):
        nonlocal count
        count += 1

        if count % 50 == 0:
            context.log.info(f"Progress: {count}/{total} ...")

        if text:
            prompt_tpl = "<2en> {text}"
            return inference.text_generation(prompt=prompt_tpl.format(text=text))
        return text

    df = df.with_columns(
        pl.col("review_comment_title").map_elements(translate, return_dtype=pl.String),
        pl.col("review_comment_message").map_elements(translate, return_dtype=pl.String)
    )

    with suppress(pl.exceptions.ShapeError):
        sample = (
            df.select("review_comment_title", "review_comment_message")
            .drop_nulls("review_comment_message")
            .sample(2)
        )

        context.log.info(sample)

    with NamedTemporaryFile() as tmpfile:
        df.write_csv(tmpfile.name)
        minio.fput_object(
            "ecommerce",
            f"translated/order_reviews.{partition}.csv",
            tmpfile.name
        )
        context.log.info(f"Processed: translated/order_reviews.{partition}.csv")

    yield MaterializeResult(metadata={
        "order_reviews (len)": MetadataValue.int(total)
    })


@asset(
    deps=[AssetKey("storage_order_reviews")],
    partitions_def=monthly_partitions_def,
    compute_kind="Python",
)
async def document_order_reviews(
        context: AssetExecutionContext,
        storage: StorageResource,
        document: DocumentResource,
        global_: GlobalResource,
):
    partition = context.partition_key
    orders = storage.read_csv("raw/orders.csv")
    order_reviews = storage.read_csv(f"translated/order_reviews.{partition}.csv")

    order_events = (
        order_reviews
        .join(orders, on="order_id", how="left")
        .with_columns(
            pl.col("order_purchase_timestamp").str.to_datetime(),
            pl.col("review_creation_date").str.to_datetime(),
            pl.col("review_answer_timestamp").str.to_datetime(),
        )
        .filter(
            pl.col("order_purchase_timestamp").dt.strftime("%Y-%m") == partition,
        )
        .drop_nulls(["order_purchase_timestamp"])
    )

    review_created_columns = [
        "review_id",
        "order_id",
        "review_creation_date",
    ]
    review_created_events = calculate_ttl_from(order_events.select(
        *review_created_columns,
        timestamp="review_creation_date",
        purchased_timestamp="order_purchase_timestamp",
    ))

    review_answered_columns = [
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_answer_timestamp",
        "review_creation_date",
    ]
    review_answered_events = calculate_ttl_from(order_events.select(
        *review_answered_columns,
        timestamp="review_answer_timestamp",
        purchased_timestamp="order_purchase_timestamp",
    ))

    context.log.info(review_created_events.describe())
    context.log.info(review_answered_events.describe())

    client = document.get_client()
    database = client["ecommerce"]
    collection = database["order_reviews"]

    if global_.delete:
        collection.delete_many({})

    if global_.drop:
        collection.drop()

    async def process_review_created(event: dict):
        await asyncio.sleep(event["ttl"])
        null_columns = set(review_answered_columns) - set(review_created_columns)
        event = {
            **{k: v for k, v in event.items() if k in review_answered_columns},
            **{k: None for k in null_columns}  # Ensure schema for later processes
        }

        collection.update_one(
            {"review_id": event["review_id"], "order_id": event["order_id"]},
            {'$set': event},
            upsert=True
        )

    async def process_review_answered(event: dict):
        await asyncio.sleep(event["ttl"])
        event = {k: v for k, v in event.items() if k in review_answered_columns}
        collection.update_one(
            {"review_id": event["review_id"], "order_id": event["order_id"]},
            {'$set': event},
            upsert=True
        )

    async def main():
        review_created_tasks = [process_review_created(event) for event in review_created_events.to_dicts()]
        review_answered_tasks = [process_review_answered(event) for event in review_answered_events.to_dicts()]
        await asyncio.gather(*review_created_tasks, *review_answered_tasks)
        client.close()

    await main()

    yield MaterializeResult(metadata={
        "review_created_events (len)": MetadataValue.int(len(review_created_events)),
        "review_answered_events (len)": MetadataValue.int(len(review_answered_events)),
    })
