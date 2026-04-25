from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as fsum, countDistinct, count, avg, datediff
)

spark = (
    SparkSession.builder
    .appName("silver-to-gold-delta")
    .getOrCreate()
)

base = "/opt/spark/data"

orders = spark.read.parquet(f"{base}/silver/orders")
order_items = spark.read.parquet(f"{base}/silver/order_items")
payments = spark.read.parquet(f"{base}/silver/payments")
shipments = spark.read.parquet(f"{base}/silver/shipments")


def write_delta(df, path, partition_col):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(partition_col)
        .save(path)
    )


# --------------------------------
# Gold 1: Daily store sales
# --------------------------------
daily_store_sales = (
    orders
    .groupBy("order_date", "store")
    .agg(
        countDistinct("order_id").alias("orders"),
        fsum("grand_total").alias("gross_sales"),
        avg("grand_total").alias("avg_order_value")
    )
)

write_delta(
    daily_store_sales,
    f"{base}/gold_delta/daily_store_sales",
    "order_date"
)

# --------------------------------
# Gold 2: Product sales
# --------------------------------
product_sales = (
    order_items
    .groupBy("order_date", "sku", "product_name", "category")
    .agg(
        fsum("quantity").alias("units_sold"),
        fsum("line_total").alias("revenue")
    )
)

write_delta(
    product_sales,
    f"{base}/gold_delta/product_sales",
    "order_date"
)

# --------------------------------
# Gold 3: Payment summary
# --------------------------------
payment_summary = (
    payments
    .groupBy("payment_date", "method", "status")
    .agg(
        count("*").alias("payment_count"),
        fsum("amount").alias("payment_amount")
    )
)

write_delta(
    payment_summary,
    f"{base}/gold_delta/payment_summary",
    "payment_date"
)

# --------------------------------
# Gold 4: Order fulfillment
# --------------------------------
delivered_shipments = (
    shipments
    .filter(col("status") == "DELIVERED")
    .select("order_id", "shipment_ts", "shipment_date")
)

order_fulfillment = (
    orders.alias("o")
    .join(delivered_shipments.alias("s"), on="order_id", how="left")
    .select(
        col("o.order_id"),
        col("o.order_date"),
        col("o.store"),
        col("o.customer_id"),
        col("o.grand_total"),
        col("s.shipment_date"),
        datediff(col("s.shipment_date"), col("o.order_date")).alias("days_to_deliver")
    )
)

write_delta(
    order_fulfillment,
    f"{base}/gold_delta/order_fulfillment",
    "order_date"
)

spark.stop()
