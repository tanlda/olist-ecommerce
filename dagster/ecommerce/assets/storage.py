import kaggle
import polars as pl

from dagster import asset, AssetExecutionContext
from ecommerce.resources import StorageResource
from ecommerce.partitions import monthly_partitions_def


@asset(
    deps=[],
    compute_kind="Python",
)
def storage_ecommerce(context: AssetExecutionContext, storage: StorageResource):
    # Download Kaggle datasets to Minio
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files("olistbr/brazilian-ecommerce", path="data/brazilian-ecommerce", unzip=True)

    client = storage.get_client()
    table_names = [
        "customers",
        "geolocation",
        "order_items",
        "order_payments",
        "order_reviews",
        "orders",
        "products",
        "sellers",
    ]

    for table_name in table_names:
        object_name = f"raw/{table_name}.csv"
        file_path = f"data/brazilian-ecommerce/olist_{table_name}_dataset.csv"
        client.fput_object(storage.bucket_name, object_name, file_path)

    # Register partitions from the dataset
    orders = pl.read_csv("data/brazilian-ecommerce/olist_orders_dataset.csv")
    temp = (
        orders
        .select(timestamp="order_purchase_timestamp")
        .with_columns(pl.col("timestamp").str.to_datetime())
        .filter(pl.col("timestamp").dt.year() != 2016)  # Start from 2017
        .select(pl.date_range(pl.min("timestamp"), pl.max("timestamp"), interval="1mo"))
        .with_columns(pl.col("timestamp").dt.strftime('%Y-%m').unique())
        .sort("timestamp")
    )
    context.instance.add_dynamic_partitions(
        partitions_def_name=monthly_partitions_def.name,
        partition_keys=temp["timestamp"].to_list(),
    )
