from dagster import Definitions, FilesystemIOManager, load_assets_from_modules, load_asset_checks_from_modules
from dagstermill import ConfigurableLocalOutputNotebookIOManager as NotebookIOManager

from ecommerce.assets import storage, database, document, lakehouse, warehouse
from ecommerce.resources import (
    LocalResource,
    GlobalResource,
    StorageResource,
    DatabaseResource,
    DocumentResource,
    WarehouseResource,
    LakehouseResource,
    IcebergResource,
    PyDeequResource,
)
from ecommerce.jobs import (
    database_assets_job,
    lakehouse_assets_job,
    warehouse_assets_job,
)
from ecommerce.schedules import (
    database_assets_schedule,
    lakehouse_assets_schedule,
    warehouse_assets_schedule
)

all_assets = [
    *load_assets_from_modules([storage], group_name="storage"),
    *load_assets_from_modules([database], group_name="database"),
    *load_assets_from_modules([document], group_name="document"),
    *load_assets_from_modules([lakehouse], group_name="lakehouse"),
    *load_assets_from_modules([warehouse], group_name="warehouse"),
]

all_asset_checks = [

]

all_jobs = [
    database_assets_job,
    lakehouse_assets_job,
    warehouse_assets_job,
]

all_schedules = [
    # database_assets_schedule,
    lakehouse_assets_schedule,
    warehouse_assets_schedule,
]

all_sensors = [
]

defs = Definitions(
    assets=all_assets,
    sensors=all_sensors,
    schedules=all_schedules,
    asset_checks=all_asset_checks,
    jobs=all_jobs,
    resources={
        "local_": LocalResource(),
        "global_": GlobalResource(),
        "io_manager": FilesystemIOManager(
            base_dir="/opt/dagster/dagster_home/storage"
        ),
        "output_notebook_io_manager": NotebookIOManager(),
        "storage": StorageResource(
            endpoint="storage.io",
            bucket_name="ecommerce",
            region_name="us-east-1",
            access_key="admin",
            secret_key="password",
        ),
        "database": DatabaseResource(
            host="database.io",
            database="ecommerce",
            username="admin",
            password="password",
        ),
        "document": DocumentResource(
            host="document.io",
            username="admin",
            password="password",
        ),
        "lakehouse": LakehouseResource(
            region_name="ap-southeast-1",
            endpoint="lakehouse.io",
            bucket_name="ecommerce",
            access_key="admin",
            secret_key="password",
        ),
        "warehouse": WarehouseResource(
            host="warehouse.io",
            database="ecommerce",
            username="admin",
            password="password",
        ),
        "iceberg": IcebergResource(
            access_key="admin",
            secret_key="password",
            uri="http://iceberg.io",
            endpoint="http://lakehouse.io",
            warehouse="s3://ecommerce/iceberg/",
        ),
        "pydeequ": PyDeequResource()
    }
)
