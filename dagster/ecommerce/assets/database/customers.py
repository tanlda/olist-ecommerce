from dagster import (
    asset,
    AssetKey,
    AssetExecutionContext,
)
from ecommerce.resources import DatabaseResource, StorageResource


@asset(
    deps=[AssetKey("storage_ecommerce")],
    compute_kind="Postgres",
)
def database_customers(context: AssetExecutionContext, database: DatabaseResource, storage: StorageResource):
    df = storage.read_csv("raw/customers.csv")
    df.write_database(
        table_name="customers",
        connection=database.get_connection(),
        if_table_exists="replace",
        engine="sqlalchemy",
    )
