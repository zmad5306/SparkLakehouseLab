from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, to_timestamp, upper,
    explode, to_date
)

spark = (
    SparkSession.builder
    .appName("bronze-to-silver")
    .getOrCreate()
)

base = "/opt/spark/data"

# -------------------------
# Orders
# -------------------------
orders_raw = (
    spark.read.json(f"{base}/raw/orders/*.json")
    .withColumn("_ingest_ts", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

orders_silver = (
    orders_raw
    .withColumn("order_ts", to_timestamp("order_ts"))
    .withColumn("status", upper(col("status")))
    .withColumn("order_date", to_date("order_ts"))
    .filter(col("order_id").isNotNull())
)

(
    orders_silver.write
    .mode("overwrite")
    .partitionBy("order_date")
    .parquet(f"{base}/silver/orders")
)

# -------------------------
# Order Items (flattened from orders)
# -------------------------
order_items_silver = (
    orders_silver
    .select(
        "order_id",
        "customer_id",
        "store",
        "order_ts",
        "order_date",
        explode("items").alias("item")
    )
    .select(
        "order_id",
        "customer_id",
        "store",
        "order_ts",
        "order_date",
        col("item.sku").alias("sku"),
        col("item.product_name").alias("product_name"),
        col("item.category").alias("category"),
        col("item.quantity").alias("quantity"),
        col("item.unit_price").alias("unit_price"),
        col("item.line_total").alias("line_total")
    )
)

(
    order_items_silver.write
    .mode("overwrite")
    .partitionBy("order_date")
    .parquet(f"{base}/silver/order_items")
)

# -------------------------
# Payments
# -------------------------
payments_raw = (
    spark.read.json(f"{base}/raw/payments/*.json")
    .withColumn("_ingest_ts", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

payments_silver = (
    payments_raw
    .withColumn("payment_ts", to_timestamp("payment_ts"))
    .withColumn("status", upper(col("status")))
    .withColumn("payment_date", to_date("payment_ts"))
    .filter(col("payment_id").isNotNull())
)

(
    payments_silver.write
    .mode("overwrite")
    .partitionBy("payment_date")
    .parquet(f"{base}/silver/payments")
)

# -------------------------
# Shipments
# -------------------------
shipments_raw = (
    spark.read.json(f"{base}/raw/shipments/*.json")
    .withColumn("_ingest_ts", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

shipments_silver = (
    shipments_raw
    .withColumn("shipment_ts", to_timestamp("shipment_ts"))
    .withColumn("status", upper(col("status")))
    .withColumn("shipment_date", to_date("shipment_ts"))
    .filter(col("shipment_id").isNotNull())
)

(
    shipments_silver.write
    .mode("overwrite")
    .partitionBy("shipment_date")
    .parquet(f"{base}/silver/shipments")
)

spark.stop()
