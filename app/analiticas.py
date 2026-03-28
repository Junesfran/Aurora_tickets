from pyspark.sql.functions import sum
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, count, when, avg, to_date

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("Consultar datos") \
    .master("spark://172.31.68.230:7077") \
    .getOrCreate()

BASE = f"s3a://huguitopesadilla/"

df = spark.read.parquet(f"{BASE}/curated/")
transactions = spark.read.parquet(f"{BASE}/curated/transactions/")

# Extraer fecha para agrupar
df = df.withColumn("dt", to_date("timestamp"))

funnel = df.groupBy("dt").agg(
    countDistinct("session_id").alias("sessions_total"),
    countDistinct(when(col("event") == "list", col("session_id"))).alias("sessions_event_list"),
    countDistinct(when(col("event") == "detail", col("session_id"))).alias("sessions_event_detail"),
    countDistinct(when(col("event") == "checkout", col("session_id"))).alias("sessions_begin_checkout"),
    countDistinct(when(col("event") == "purchase", col("session_id"))).alias("sessions_purchase")
)

# Conversion rate
funnel = funnel.withColumn(
    "conversion_rate",
    when(col("sessions_total") > 0,
         col("sessions_purchase") / col("sessions_total")
    ).otherwise(0)
)


# 1. INTERÉS → detail views (clickstream)
detail = df.filter(col("event") == "detail") \
    .groupBy("dt", "event_id") \
    .agg(count("*").alias("detail_views"))

# 2. COMPRAS + REVENUE → desde transactions
purchases = transactions.groupBy("dt", "event_id") \
    .agg(
        count("*").alias("purchases"),
        sum("amount").alias("revenue_total")
    )

# 3. JOIN
top_events = detail \
    .join(purchases, ["dt", "event_id"], "left") \
    .fillna(0)

# 4. RATIO interés → compra
top_events = top_events.withColumn(
    "interest_to_purchase_ratio",
    when(col("detail_views") > 0,
         col("purchases") / col("detail_views")
    ).otherwise(0)
)


anomalies = df.groupBy("dt", "ip").agg(
    count("*").alias("requests")
)

# Media global (detección simple)
avg_requests = anomalies.select(avg("requests")).collect()[0][0]

anomalies = anomalies.withColumn(
    "is_anomaly",
    col("requests") > avg_requests * 3
).withColumn(
    "reason",
    when(col("requests") > avg_requests * 3,
         "High traffic vs average (possible bot)")
    .otherwise("normal")
)

funnel.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(f"{BASE}/analytics/funnel/")

top_events.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(f"{BASE}/analytics/top_events/")

anomalies.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet(f"{BASE}/analytics/anomalies/")

# Activa esto solo si tu RDS ya está listo
jdbc_url = "jdbc:mysql://aurora.cluster-cpymi28a882x.us-east-1.rds.amazonaws.com:3306/aurora"

properties = {
    "user": "admin",
    "password": "Aurora123!",
    "driver": "com.mysql.cj.jdbc.Driver"
}

funnel.write.jdbc(jdbc_url, "metrics_funnel_daily", mode="append", properties=properties)
top_events.write.jdbc(jdbc_url, "metrics_event_rank", mode="append", properties=properties)
anomalies.write.jdbc(jdbc_url, "metrics_anomalies", mode="append", properties=properties)

spark.stop()
