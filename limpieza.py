from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("Aurora Job 1") \
    .master("spark://172.31.68.230:7077") \
    .getOrCreate()

# =========================
# 1. LECTURA (RAW)
# =========================

df_click = spark.read.json("s3a://huguitopesadilla/raw/aurora_clickstream.jsonl")

df_events = spark.read.option("header", True).csv("s3a://huguitopesadilla/raw/events.csv")
df_campaigns = spark.read.option("header", True).csv("s3a://huguitopesadilla/raw/campaigns.csv")
df_transactions = spark.read.option("header", True).csv("s3a://huguitopesadilla/raw/transactions.csv")

# =========================
# 2. LIMPIEZA CLICKSTREAM
# =========================

df_click_clean = df_click \
    .dropna(subset=["session_id", "event_type"]) \
    .filter(col("event_id").isNotNull())

# Convertir timestamp → fecha (dt)
df_click_clean = df_click_clean \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("dt", to_date(col("timestamp")))

# =========================
# 3. LIMPIEZA CSVs
# =========================

df_events_clean = df_events.dropna(subset=["event_id"])
df_campaigns_clean = df_campaigns.dropna(subset=["utm_campaign"])
df_transactions_clean = df_transactions.dropna(subset=["session_id", "event_id"])

# =========================
# 4. NORMALIZAR TIPOS
# =========================

df_events_clean = df_events_clean.withColumn("event_id", col("event_id").cast("int"))
df_transactions_clean = df_transactions_clean.withColumn("event_id", col("event_id").cast("int"))

# =========================
# 5. ESCRITURA (CURATED)
# =========================

# Clickstream limpio
df_click_clean.write \
    .mode("overwrite") \
    .partitionBy("dt", "event_type") \
    .parquet("s3a://huguitopesadilla/curated/clickstream/")

# CSVs limpios
df_events_clean.write.mode("overwrite").parquet("s3a://huguitopesadilla/curated/events/")
df_campaigns_clean.write.mode("overwrite").parquet("s3a://huguitopesadilla/curated/campaigns/")
df_transactions_clean.write.mode("overwrite").parquet("s3a://huguitopesadilla/curated/transactions/")

# =========================
# FIN
# =========================

spark.stop()