# BRONZE LAYER — Spark Declarative Pipeline (SDP/Lakeflow)
# SDP CONCEPTS DEMONSTRATED:
#   1. dlt.create_streaming_table()   — defines a streaming target table
#   2. dlt.append_flow()              — appends Kafka stream to bronze table
#   3. dlt.expect_or_drop()           — data quality gate
#   4. Continuous mode                — runs 24/7, low-latency ingestion

# WHY BRONZE IS APPEND-ONLY:
#   Bronze is the "raw vault" — we NEVER modify or delete raw records.
#   It's the source of truth for replay, debugging, and audit.
#   append_flow() enforces this: you can only add rows, never update.

# ARCHITECTURE ADVANTAGE:
#   Using SDP instead of raw Spark streaming code gives us:
#   - Automatic dependency resolution (pipeline graph)
#   - Built-in checkpointing (no manual checkpoint management)
#   - Automatic schema evolution
#   - Data quality metrics in Databricks UI
#   - One-click pipeline monitoring


import dlt                          # Spark Declarative Pipelines API
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, IntegerType, TimestampType, LongType
)

# --- ENTERPRISE DATA QUALITY FRAMEWORK ---
import sys
import os

# Tell Python to look in the transformations/ folder
sys.path.append(os.path.abspath('/Workspace/Repos/nandhuvinoth95@gmail.com/fintech-fraud-detection/Pipeline/transformations'))

from data_quality.data_quality import BRONZE_EXPECTATIONS, add_audit_columns

# TRANSACTION SCHEMA DEFINITION

# Explicit schema = faster reads + prevents schema inference overhead
# In production, use schema registry for schema evolution governance

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",        StringType(),    False),
    StructField("customer_id",           StringType(),    False),
    StructField("merchant_id",           StringType(),    False),
    StructField("amount",                DoubleType(),    False),
    StructField("currency",              StringType(),    True),
    StructField("timestamp",             StringType(),    True),
    StructField("transaction_date",      StringType(),    True),
    StructField("location_city",         StringType(),    True),
    StructField("location_state",        StringType(),    True),
    StructField("location_country",      StringType(),    True),
    StructField("latitude",              DoubleType(),    True),
    StructField("longitude",             DoubleType(),    True),
    StructField("ip_address",            StringType(),    True),
    StructField("device_id",             StringType(),    True),
    StructField("device_type",           StringType(),    True),
    StructField("payment_method",        StringType(),    True),
    StructField("transaction_status",    StringType(),    True),
    StructField("merchant_category",     StringType(),    True),
    StructField("is_foreign_transaction",BooleanType(),   True),
    StructField("failed_attempt_count",  IntegerType(),   True),
    StructField("response_code",         StringType(),    True),
    StructField("hour_of_day",           IntegerType(),   True),
    StructField("day_of_week",           IntegerType(),   True),
    StructField("is_weekend",            BooleanType(),   True),
    StructField("is_night_transaction",  BooleanType(),   True),
    StructField("is_fraud",              BooleanType(),   True),
    StructField("fraud_type",            StringType(),    True),
    StructField("fraud_score",           DoubleType(),    True),
    StructField("_kafka_produced_at",    StringType(),    True),
    StructField("_schema_version",       StringType(),    True),
])


# PIPELINE PARAMETERS (injected by Databricks Pipeline config)
def get_pipeline_param(key: str, default: str = "") -> str:
    """Read pipeline parameters with fallback defaults (Serverless-compatible)."""
    return default


# ============================================================
# 1. LIVE KAFKA STREAM
# ============================================================
@dlt.table(
    name    = "bronze_transactions_raw",
    comment = "Raw transaction events from Kafka — append-only, no deduplication",
    table_properties = {
        "quality":                       "bronze",
        "delta.enableChangeDataFeed":    "true",   # Enable CDC for downstream
        "pipelines.autoOptimize.managed":"true",
        "delta.dataSkippingNumIndexedCols": "5",
    }
)
@dlt.expect_all(BRONZE_EXPECTATIONS)
def bronze_transactions_raw():
    kafka_bootstrap = get_pipeline_param(
        "kafka_bootstrap_servers", "tkvub-117-254-32-118.run.pinggy-free.link:37231"
    )
    kafka_topic = get_pipeline_param(
        "kafka_topic_transactions", "fraud.transactions"
    )

    raw_stream = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", kafka_bootstrap)
             .option("subscribe", kafka_topic)
             .option("startingOffsets", "latest")   
             .option("maxOffsetsPerTrigger", 50000) 
             .option("failOnDataLoss", "false")      
             .load()
    )

    parsed = (
        raw_stream
        .select(
            F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
            F.col("topic").alias("_kafka_topic"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").cast(LongType()).alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
        )
        .select("data.*", "_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp")
    )

    enriched = (
        parsed
        .withColumn("_ingested_at",       F.current_timestamp())
        .withColumn("_ingested_date",     F.current_date())
        .withColumn("_source_system",     F.lit("KAFKA"))
        .withColumn("event_timestamp",    F.col("timestamp").cast("timestamp"))
        .withColumn("transaction_date",   F.col("transaction_date").cast("date"))
        .withWatermark("event_timestamp", "30 minutes")
    )

    return add_audit_columns(enriched, "BRONZE", "fraud_engine_v1")


# ============================================================
# 2. S3 HISTORICAL BACKFILL (Parquet Fix Applied)
# ============================================================
@dlt.table(
    name    = "bronze_transactions_s3",
    comment = "Raw transactions from S3 landing zone via Auto Loader",
    table_properties = {
        "quality":                    "bronze",
        "delta.enableChangeDataFeed": "true",
    }
)
@dlt.expect_all(BRONZE_EXPECTATIONS)
def bronze_transactions_s3():
    landing_path = get_pipeline_param(
        "s3_landing_path",
        "s3://fintech-fraud-detection-lake/landing/transactions"
    )

    raw = (
        spark.readStream
             .format("cloudFiles")                          
             .option("cloudFiles.format", "parquet")      
             .option("cloudFiles.inferColumnTypes", "true")
             .option("cloudFiles.maxFilesPerTrigger", "10") 
             .load(landing_path)
             .withColumn("_ingested_at",    F.current_timestamp())
             .withColumn("_source_system",  F.lit("S3_AUTO_LOADER"))
             .withColumn("_source_file",    F.col("_metadata.file_path"))
             .withColumn("event_timestamp", F.col("timestamp").cast("timestamp"))
             .withColumn("transaction_date", F.col("transaction_date").cast("date"))
             .withWatermark("event_timestamp", "30 minutes")
    )
    
    # Ensure S3 fallback gets the audit framework.
    return add_audit_columns(raw, "BRONZE", "fraud_engine_v1")


# ============================================================
# 3. BRONZE CUSTOMERS — Batch S3 ingestion
# ============================================================
@dlt.table(
    name    = "bronze_customers_raw",
    comment = "Customer profiles from S3 — source for SCD Type 2",
    table_properties = {
        "quality":                    "bronze",
        "delta.enableChangeDataFeed": "true",
    }
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_kyc_status", "kyc_status IN ('VERIFIED', 'PENDING', 'FAILED')")
def bronze_customers_raw():
    raw_customers = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("cloudFiles.inferColumnTypes", "true")
             .option("header", "true")
             .load("s3://fintech-fraud-detection-lake/landing/customers")
             .withColumn("_ingested_at",   F.current_timestamp())
             .withColumn("_source_system", F.lit("S3_CUSTOMERS"))
             .withColumn("_source_file",   F.col("_metadata.file_path"))
             .withColumn("_batch_id",      F.lit(None).cast("string"))
    )
    
    return add_audit_columns(raw_customers, "BRONZE", "fraud_engine_v1")


# ============================================================
# 4. BRONZE MERCHANTS — Batch ingestion
# ============================================================
@dlt.table(
    name    = "bronze_merchants_raw",
    comment = "Merchant profiles from S3",
    table_properties = {
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
    }
)
@dlt.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
def bronze_merchants_raw():
    raw_merchants = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("cloudFiles.inferColumnTypes", "true")                    
             .option("header", "true")
             .load("s3://fintech-fraud-detection-lake/landing/merchants")
             .withColumn("_ingested_at",   F.current_timestamp())
             .withColumn("_source_system", F.lit("S3_MERCHANTS"))
    )
    
    return add_audit_columns(raw_merchants, "BRONZE", "fraud_engine_v1")


# ============================================================
# 5. DEAD LETTER TABLE — Malformed records quarantine
# ============================================================
@dlt.table(
    name    = "bronze_transactions_dead_letter",
    comment = "Quarantined malformed/rejected transaction records",
    table_properties = {"quality": "quarantine"}
)
def bronze_transactions_dead_letter():
    landing_path = "s3://fintech-fraud-detection-lake/landing/transactions"

    raw = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .schema(TRANSACTION_SCHEMA)
             .load(landing_path)
    )

    dead_letter = (
        raw
        .filter(
            F.col("transaction_id").isNull() |
            F.col("amount").isNull() |
            (F.col("amount").cast("double") <= 0)
        )
        .withColumn("_quarantine_reason",
            F.when(F.col("transaction_id").isNull(), "NULL_TRANSACTION_ID")
             .when(F.col("amount").isNull(), "NULL_AMOUNT")
             .otherwise("INVALID_AMOUNT")
        )
        .withColumn("_quarantined_at", F.current_timestamp())
    )
    
    return add_audit_columns(dead_letter, "BRONZE", "fraud_engine_v1")


# ============================================================
# 6. BRONZE FX RATES — Daily batch ingestion
# ============================================================
@dlt.table(
    name    = "bronze_daily_fx_rates",
    comment = "Daily FX rates ingested from S3 via Auto Loader",
    table_properties = {
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_currency", "currency_code IS NOT NULL")
def bronze_daily_fx_rates():
    fx_rates = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("header", "true")
             .option("cloudFiles.inferColumnTypes", "true") 
             .load("s3://fintech-fraud-detection-lake/landing/fx_rates")
             .withColumn("_ingested_at", F.current_timestamp())
             .withColumn("_source_system", F.lit("S3_FX_RATES"))
             .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    
    return add_audit_columns(fx_rates, "BRONZE", "fraud_engine_v1")
