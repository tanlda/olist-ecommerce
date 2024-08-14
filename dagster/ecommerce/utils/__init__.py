import polars as pl


def calculate_ttl_from(df: pl.DataFrame) -> pl.DataFrame:
    # compress_factor = 1 / (24 * 60)  # 1 day = 1 min
    # compress_factor = 1 / (24 * 60) / 6  # 1 day = 10 sec
    compress_factor = 1 / (24 * 60) / 6 / 10  # 1 day = 1 sec

    return (
        df
        .with_columns(duration=pl.col("timestamp") - pl.min("purchased_timestamp"))
        .with_columns(
            ttl=(pl.col("duration").dt.total_nanoseconds() / 1e9 * compress_factor).cast(pl.Float64),
        )
        .drop_nulls("timestamp")
        .sort("ttl")
    )
