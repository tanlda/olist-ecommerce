# End-to-End Data Engineering Project: Olist Ecommerce

![image](https://github.com/user-attachments/assets/1450f899-f8d3-4748-a300-4b4278d0a6ae)

## Project Overview
This project aims to build a comprehensive data pipeline for an e-commerce platform, providing hands-on experience with various data engineering tools and technologies. The pipeline will ingest data from multiple sources, process it, and store it in different formats for analysis and reporting.

## Key Components:

### 1. Data Sources
- The e-commerce data set provided in this project is from [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) on Kaggle.
- Tables as CSV files are first downloaded using [Kaggle API](https://pypi.org/project/kaggle/), stored in [Minio](https://min.io/) (storage.io), orchestated by [Dagster](https://dagster.io/).

### 2. Data Generation
- Tables that have timestamp columns (such as orders, orders_reviews, ...) are then partitioned by month with [Dagster Partitions](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions).
- After that monthly data is then inserted and updated to different locations: [PostgreSQL](https://www.postgresql.org/) (database.io) and [MongoDB](https://www.mongodb.com/) (document.io), based on the timestamp of corresponding events. This process is to simulate the real-time data ingestion, emitting changes for CDC in later steps with Kafka.
- For tables without timestamp, ingest directly to the PostgreSQL database.
- This process is orchestarted by [Dagster](https://dagster.io/) with [asyncio](https://docs.python.org/3/library/asyncio.html).

### 3. Data Pipelines (Batch Layer)
- Airflow ETL dags with 5-minute intervals cronjob (mimic daily jobs) are setup to:
  - Extract daily data from the PostgreSQL and MongoDB mentioned in the previous step using [Polars](https://pola.rs/), and then upload to another [Minio](https://min.io/) instance (lakehouse.io) under `/ecommerce/landing/` prefix.
  - Spinup Spark jobs using [Airflow SparkSubmitOperator](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/operators/spark_submit/index.html) to read extracted CSV files and then merge to the corresponding [Iceberg](https://iceberg.apache.org/) table in idempotent manner.
  - Lastly, Spark jobs are submmited to retrieve records from Iceberg table, then merge to another PostgreSQL instance (warehouse.io). This is to allow BI applications such as [Apache Superset](https://superset.apache.org/) or [Power BI](https://www.microsoft.com/en-us/power-platform/products/power-bi) can easily integrate and build analytic dashboards.
- The process is orchestarted by [Airflow](https://airflow.apache.org/).

### 4. Data Streaming (Real-Time Layer)
- Changes being made in Data Generation sources (database.io and document.io) are captured by [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html) with [Debezium](https://debezium.io/) capability, CDC messages are then delivery to a [Strimzi Kafka](https://strimzi.io/) cluster.
- From here, messages in Kafka topics are consumed using [Flink](https://flink.apache.org/) (Table API) and then ingest to [OpenSearch](https://opensearch.org/) instance with the corresponding indices.

### 5. Cloud Platform (AWS)
- [AWS DMS](https://aws.amazon.com/dms/) are setup to migrate tables in PostgreSQL to an [RDS Postgres](https://aws.amazon.com/rds/postgresql/) instance, while documents in MongoDB are migrated to an [S3](https://aws.amazon.com/s3/) bucket.
- [Glue Data Catalog](https://docs.aws.amazon.com/prescriptive-guidance/latest/serverless-etl-aws-glue/aws-glue-data-catalog.html) and [Glue ETL](https://aws.amazon.com/glue/) are used to extract and load data from the above step to an [Redshift](https://aws.amazon.com/redshift/) data warehouse instance.
- With data in [Redshift](https://aws.amazon.com/redshift/), larger datasets can be queried and proccessed without limitations as compare to local PostgreSQL as data warehouse.

### 6. Data Visualization
- [Superset](https://superset.apache.org/) is use to create analytic dashboards from PostgreSQL (warehouse.io) as data warehouse.

### 7. Infrastructure
- Services are deployed on a local [Kubernetes](https://kubernetes.io/) cluster of 2 physical machines, with the help of various toolings such as: [Docker](https://www.docker.com/), [Kubeadm](https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/create-cluster-kubeadm/), [K9s](https://k9scli.io/), [Helm](https://helm.sh/), [Helmfile](https://helmfile.readthedocs.io/en/latest/), [Taskfile](https://taskfile.dev/) and Bash scripts.
- - [Terraform](https://www.terraform.io/) with [Terragrunt](https://terragrunt.gruntwork.io/) wrapper is used to deploy resources on [AWS](https://aws.amazon.com/) as well as [Cloudflare](https://www.cloudflare.com/).

### 8. Miscellaneous
- Jupyter Notebooks are used to test implementations and develop data pipelines, as interactive environment provide faster feedback loops hence allow for faster development.
- [DataHub](https://datahubproject.io/) is deploy as a proof of concept to manage, discovery and governance all datasets in the datastack as well as their data lineages.

### 9. Screenshots
- Screenshots of the projects can be found under [screenshots](https://github.com/tanlda/olist-ecommerce/tree/main/screenshots) directory.
