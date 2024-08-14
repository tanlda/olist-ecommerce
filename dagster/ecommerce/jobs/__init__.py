from dagster import (
    define_asset_job,
    AssetSelection,
)

database_assets_job = define_asset_job(
    name="database_job",
    selection=AssetSelection.assets(
        "database_orders",
        "database_order_items",
        "database_order_payments",
        "document_order_reviews",
    )
)

lakehouse_assets_job = define_asset_job(
    name="lakehouse_job",
    selection=AssetSelection.assets(
        "lakehouse_orders",
        "lakehouse_order_items",
        "lakehouse_order_payments",
        "lakehouse_order_reviews",
    )
)

warehouse_assets_job = define_asset_job(
    name="warehouse_job",
    selection=AssetSelection.assets(
        "warehouse_orders",
        "warehouse_order_items",
        "warehouse_order_payments",
        "warehouse_order_reviews",
    )
)
