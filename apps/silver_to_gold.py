from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, countDistinct, datediff, sum as fsum

spark = (
    SparkSession.builder
    .appName("silver-to-gold")
    .getOrCreate()
)

base = "/opt/spark/data"

orders = spark.read.parquet(f"{base}/silver/orders")
order_items = spark.read.parquet(f"{base}/silver/order_items")
payments = spark.read.parquet(f"{base}/silver/payments")
shipments = spark.read.parquet(f"{base}/silver/shipments")


def write_parquet(df, path, partition_col):
    (
        df.write
        .mode("overwrite")
        .partitionBy(partition_col)
        .parquet(path)
    )


daily_store_sales = (
    orders
    .groupBy("order_date", "store")
    .agg(
        countDistinct("order_id").alias("orders"),
        fsum("grand_total").alias("gross_sales"),
        avg("grand_total").alias("avg_order_value")
    )
)

write_parquet(
    daily_store_sales,
    f"{base}/gold/daily_store_sales",
    "order_date"
)

product_sales = (
    order_items
    .groupBy("order_date", "sku", "product_name", "category")
    .agg(
        fsum("quantity").alias("units_sold"),
        fsum("line_total").alias("revenue")
    )
)

write_parquet(
    product_sales,
    f"{base}/gold/product_sales",
    "order_date"
)

payment_summary = (
    payments
    .groupBy("payment_date", "method", "status")
    .agg(
        count("*").alias("payment_count"),
        fsum("amount").alias("payment_amount")
    )
)

write_parquet(
    payment_summary,
    f"{base}/gold/payment_summary",
    "payment_date"
)

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

write_parquet(
    order_fulfillment,
    f"{base}/gold/order_fulfillment",
    "order_date"
)

spark.stop()
