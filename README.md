# Spark Lakehouse Lab

Local hands-on lab for learning a small medallion-style lakehouse with Apache Spark, Parquet, Delta Lake, Spark SQL, the Spark Thrift Server, and DBeaver.

The repo is intentionally simple and learning-first. You generate synthetic ecommerce data, transform it into a cleaned Silver layer, publish Gold datasets as Delta tables, and optionally query them through a SQL client without hiding the moving parts behind a managed platform.

## What This Lab Covers

The dataset models a small "tiny online store analytics" use case with:

* Orders
* Nested order line items
* Payments
* Shipments

The pipeline produces:

| Layer | Path | Format | Purpose |
| --- | --- | --- | --- |
| Raw | `data/raw` | NDJSON | Source-style landing data |
| Silver | `data/silver` | Parquet | Cleaned, typed, and flattened datasets |
| Gold | `data/gold_delta` | Delta Lake | Business-ready analytical tables with transaction logs |
| SQL Access | Spark Thrift Server | JDBC / HiveServer2 | Query Delta tables from DBeaver |

## Learning Goals

This lab is designed to help you get comfortable with the separation between:

* raw landing data vs curated analytical data
* file formats like JSON and Parquet vs table semantics from Delta Lake
* Spark as a transformation engine vs a serving/query layer for interactive use

If you want the shortest mental model:

* Bronze or Raw keeps the facts, even if they are messy
* Silver makes the data usable
* Gold makes the data analytically useful
* a serving layer makes Gold convenient to query

## Architecture

```text
Synthetic JSON events
  -> data/raw
  -> Silver Parquet tables
  -> Gold Delta tables
  -> Spark Thrift Server
  -> DBeaver / JDBC client
```

## Repo Layout

```text
spark-lakehouse-lab/
├─ docker-compose.yml
├─ apps/
│  ├─ bronze_to_silver.py
│  ├─ generate_raw_data.py
│  └─ silver_to_gold_delta.py
├─ data/
│  ├─ raw/
│  ├─ silver/
│  ├─ gold_delta/
│  └─ .ivy2/
└─ README.md
```

## Services

`docker-compose.yml` starts four containers based on `apache/spark:4.0.1`:

* `spark-master`
* `spark-worker-1`
* `spark-worker-2`
* `spark-client`

Exposed ports:

* `8080`: Spark master UI
* `8081`: worker 1 UI
* `8082`: worker 2 UI
* `7077`: Spark standalone master
* `10000`: Spark Thrift Server

Keep an eye on the Spark UI while you work. Watching workers register and jobs appear is one of the fastest ways to make the cluster feel concrete.

## Before You Start

Requirements:

* Docker Desktop or Docker Engine with Compose

Bring the cluster up:

```bash
docker compose up -d
```

Open the Spark UI at `http://localhost:8080`.

## 1. Generate Raw Data

This creates newline-delimited JSON files under `data/raw/orders`, `data/raw/payments`, and `data/raw/shipments`.

```bash
docker exec -it spark-client python3 /opt/spark/apps/generate_raw_data.py
```

Expected output is a generated batch id plus record counts for orders, payments, and shipments.

Quick checks:

```bash
find data/raw -maxdepth 2 -type f | sort
head -n 2 data/raw/orders/*.json
```

This raw landing area is effectively the Bronze layer for the current repo.

## 2. Build the Silver Layer

The Silver job:

* reads raw JSON
* adds ingest metadata
* parses timestamps
* uppercases statuses
* derives partition dates
* flattens nested order items
* writes partitioned Parquet datasets

Run it:

```bash
docker exec -it spark-client /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/apps/bronze_to_silver.py
```

Silver outputs:

* `data/silver/orders`
* `data/silver/order_items`
* `data/silver/payments`
* `data/silver/shipments`

Verify:

```bash
find data/silver -maxdepth 2 -type d | sort
```

What you are learning in Silver:

* how Spark reads newline-delimited JSON
* how nested data becomes a flatter analytical shape
* how typed and partitioned Parquet is easier to work with than raw JSON

## 3. Build the Gold Delta Layer

This job creates analytical Gold datasets as Delta Lake tables under `data/gold_delta`.

The first run needs an Ivy cache location for downloaded Delta jars:

```bash
mkdir -p data/.ivy2
```

Run it:

```bash
docker exec -it spark-client /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf "spark.jars.ivy=/opt/spark/data/.ivy2" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  /opt/spark/apps/silver_to_gold_delta.py
```

Verify that Delta transaction logs exist:

```bash
find data/gold_delta -maxdepth 3 -type d | grep _delta_log
```

## Gold Dataset Definitions

The Gold Delta job builds these four outputs:

### `daily_store_sales`

Aggregates orders by `order_date` and `store` with:

* distinct order count
* gross sales
* average order value

### `product_sales`

Aggregates item-level demand by `order_date`, `sku`, `product_name`, and `category` with:

* units sold
* revenue

### `payment_summary`

Aggregates payments by `payment_date`, `method`, and `status` with:

* payment count
* payment amount

### `order_fulfillment`

Joins orders to delivered shipments and calculates `days_to_deliver`.

## Query the Silver Layer in PySpark

Start an interactive shell:

```bash
docker exec -it spark-client /opt/spark/bin/pyspark \
  --master spark://spark-master:7077
```

Inside PySpark:

```python
base = "/opt/spark/data"

orders = spark.read.parquet(f"{base}/silver/orders")
items = spark.read.parquet(f"{base}/silver/order_items")

orders.show(10, truncate=False)
items.show(10, truncate=False)

orders.createOrReplaceTempView("silver_orders")

spark.sql("""
select
  order_date,
  store,
  count(*) as orders,
  round(sum(grand_total), 2) as gross_sales
from silver_orders
group by order_date, store
order by order_date desc, store
limit 20
""").show(truncate=False)
```

Exit with:

```python
exit()
```

## Query the Delta Layer in PySpark

```bash
docker exec -it spark-client /opt/spark/bin/pyspark \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.13:4.0.0 \
  --conf "spark.jars.ivy=/opt/spark/data/.ivy2" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

Example:

```python
delta_sales = spark.read.format("delta").load("/opt/spark/data/gold_delta/daily_store_sales")
delta_sales.orderBy("order_date", "store").show(20, truncate=False)
```

## Start the Spark Thrift Server

The Thrift Server lets tools like DBeaver query Spark SQL over JDBC.

Start it after the Delta jars have been downloaded at least once:

```bash
docker exec -it spark-client bash -lc '
DELTA_JARS=$(echo /opt/spark/data/.ivy2/jars/*.jar | tr " " ",")
/opt/spark/sbin/start-thriftserver.sh \
  --master spark://spark-master:7077 \
  --jars "$DELTA_JARS" \
  --conf "spark.driver.extraClassPath=/opt/spark/data/.ivy2/jars/*" \
  --conf "spark.executor.extraClassPath=/opt/spark/data/.ivy2/jars/*" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
'
```

## Connect from DBeaver

Use a Hive or HiveServer2 connection:

* Host: `localhost`
* Port: `10000`
* Database: `default`
* JDBC URL: `jdbc:hive2://localhost:10000/default`

This is a useful local SQL access pattern, but it is still Spark underneath. It is great for learning and inspection, not the same thing as a purpose-built low-latency serving layer.

## Register Delta Tables for SQL

The Delta files exist on disk after the job runs, but you still need to register them as Spark SQL tables before querying them by name through the Thrift Server.

Example registrations:

```sql
CREATE TABLE IF NOT EXISTS gold_daily_store_sales
USING DELTA
LOCATION '/opt/spark/data/gold_delta/daily_store_sales';

CREATE TABLE IF NOT EXISTS gold_product_sales
USING DELTA
LOCATION '/opt/spark/data/gold_delta/product_sales';

CREATE TABLE IF NOT EXISTS gold_payment_summary
USING DELTA
LOCATION '/opt/spark/data/gold_delta/payment_summary';

CREATE TABLE IF NOT EXISTS gold_order_fulfillment
USING DELTA
LOCATION '/opt/spark/data/gold_delta/order_fulfillment';
```

Example query:

```sql
SELECT
  store,
  SUM(gross_sales) AS gross_sales
FROM gold_daily_store_sales
GROUP BY store
ORDER BY gross_sales DESC;
```

## Optional Serving Layer After Gold

If your goal is "online querying" or a more warehouse-like experience, the next step is usually to put a serving engine on top of Gold instead of pointing every query at Spark.

Two good local options:

* DuckDB: the fastest feedback loop for local analytics if you want to query exported Parquet data or a later serving copy
* Postgres: a more realistic EDW path if you want to model dimensions, facts, and indexed reporting tables

Because Gold in this repo is Delta-first, a simple local pattern is:

* use Spark SQL or Spark Thrift Server directly against Delta while learning
* or materialize/export curated data into a serving layer such as DuckDB or Postgres

Example Spark SQL registration for the Delta Gold layer:

```sql
CREATE TABLE IF NOT EXISTS gold_product_sales
USING DELTA
LOCATION '/opt/spark/data/gold_delta/product_sales';
```

That separation is worth understanding:

* Spark = transform and batch compute
* Delta Gold = curated analytical outputs
* DuckDB or Postgres = serving/query layer

## Notes on Bronze vs Raw

This repo uses `data/raw` as the actual landing zone on disk. If you think in medallion terms, that folder is effectively the Bronze layer for this lab.

There is also a `data/bronze` directory in the repo, but the current jobs do not write to it.

## Mental Model

```text
JSON     = raw events
Parquet  = efficient analytical file format
Delta    = table semantics on top of files
Spark    = compute engine
Thrift   = SQL endpoint
DBeaver  = client
Docker   = local infrastructure
```

This gives you a compact local lakehouse you can inspect end to end.
