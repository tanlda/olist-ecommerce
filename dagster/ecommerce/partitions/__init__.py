from dagster import DynamicPartitionsDefinition, DailyPartitionsDefinition

monthly_partitions_def = DynamicPartitionsDefinition(name="monthly_partitions")

daily_partitions_def = DailyPartitionsDefinition(start_date="2017-01-01", end_date="2018-12-31")
