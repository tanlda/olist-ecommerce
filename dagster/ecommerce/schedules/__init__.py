from dagster import (
    build_schedule_from_partitioned_job,
)

from ecommerce.jobs import (
    database_assets_job,
    lakehouse_assets_job,
    warehouse_assets_job,
)

database_assets_schedule = build_schedule_from_partitioned_job(
    database_assets_job,
)

lakehouse_assets_schedule = build_schedule_from_partitioned_job(
    lakehouse_assets_job,
    hour_of_day=1,
)

warehouse_assets_schedule = build_schedule_from_partitioned_job(
    warehouse_assets_job,
    hour_of_day=2,
)
