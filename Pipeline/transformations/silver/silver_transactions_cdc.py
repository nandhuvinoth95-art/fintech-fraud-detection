# pipelines/silver/silver_transactions_cdc.py
# ============================================================
# SILVER LAYER — CDC, Deduplication, Cleaning, SCD
#
# SDP CONCEPTS DEMONSTRATED:
#   1. dlt.create_auto_cdc_flow()   — declarative CDC merge
#   2. dlt.apply_changes()          — declarative SCD Type 2 history
#   3. Stream-Static Joins          — dynamic dimension enrichment
#   4. Data quality expectations    — enforced at Silver layer
# ============================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# --- ENTERPRISE DATA QUALITY FRAMEWORK ---
import sys
import os

# Tell Python to look in the transformations/ folder
sys.path.append(os.path.abspath('/Workspace/Users/nandhuvinoth95@gmail.com/New Pipeline 2026-05-22 22:38/transformations'))

from data_quality.data_quality import SILVER_EXPECTATIONS_DROP, add_audit_columns
# ============================================================
# AUTO CDC FLOW — Transaction Status Changes
# ============================================================

dlt.create_streaming_table(
    name    = "silver.silver_transactions_cleaned",
    comment = "Deduplicated, cleaned, enriched transactions — CDC maintained",
    table_properties = {
        "quality":                       "silver",
        "delta.enableChangeDataFeed":    "true",   # Feed Gold layer
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact":   "true",
    },
    expect_all = SILVER_EXPECTATIONS_DROP
)

dlt.create_auto_cdc_flow(
    name             = "silver_transactions_cdc_flow",
    source           = "bronze.bronze_transactions_raw",
    target           = "silver.silver_transactions_cleaned",
    keys             = ["transaction_id"],
    sequence_by      = F.col("_kafka_offset"),    # Kafka offset as sequence
    apply_as_deletes = None,   
    except_column_list = [
        "_kafka_topic", "_kafka_partition", "_kafka_offset",
        "_kafka_timestamp", "_kafka_produced_at", "_schema_version",
        "timestamp", "_ingested_at", "_ingested_date", "_pipeline_name"
    ],
    ignore_null_updates = True,
)


# ============================================================
# FX RATES DIMENSION — Static Snapshot for Joins
# ============================================================

@dlt.table(
    name="silver.silver_exchange_rates",
    comment="Daily FX rates normalized to USD multiplier"
)
def silver_exchange_rates():
    return dlt.read("bronze_daily_fx_rates")


# ============================================================
# ENRICHMENT VIEW — Stream-Static Join Applied AFTER CDC
# ============================================================

@dlt.view(name="silver_transactions_enriched")
def silver_transactions_enriched():
    """
    Enrich silver transactions with computed fraud signals and dynamic FX rates.
    """
    # 1. Read the continuous stream of transactions
    txns = dlt.read_stream("silver.silver_transactions_cleaned")
    
    # 2. Read the STATIC snapshot of exchange rates
    # Select ONLY the columns needed to avoid audit column collisions
    fx_rates = dlt.read("silver.silver_exchange_rates").select(
        "currency_code", "rate_date", "usd_multiplier"
    )

    # 3. Perform the Stream-Static Join
    enriched_stream = (
        txns.join(
            F.broadcast(fx_rates), 
            on=[
                txns.currency == fx_rates.currency_code,
                txns.transaction_date == fx_rates.rate_date
            ],
            how="left"
        )
        .withColumn("amount_usd", F.coalesce(F.col("amount") * F.col("usd_multiplier"), F.col("amount")))
        .drop("currency_code", "rate_date", "usd_multiplier")
        
        # ── Risk band classification ──────────────────────────────
        .withColumn("risk_band",
            F.when(F.col("fraud_score") >= 0.8, "CRITICAL")
             .when(F.col("fraud_score") >= 0.6, "HIGH")
             .when(F.col("fraud_score") >= 0.4, "MEDIUM")
             .otherwise("LOW")
        )

        # ── Amount bucketing ──────────────────────────────────────
        .withColumn("amount_band",
            F.when(F.col("amount_usd") <    100,  "MICRO (<100)")
             .when(F.col("amount_usd") <   1000,  "SMALL (100-1K)")
             .when(F.col("amount_usd") <  10000,  "MEDIUM (1K-10K)")
             .when(F.col("amount_usd") < 100000,  "LARGE (10K-100K)")
             .otherwise("VERY_LARGE (>100K)")
        )

        # ── High-risk merchant flag ───────────────────────────────
        .withColumn("is_high_risk_merchant",
            F.col("merchant_category").isin(
                "CRYPTO_EXCHANGE", "MONEY_TRANSFER", "ONLINE_GAMING", "JEWELRY", "LUXURY_GOODS"
            )
        )

        # ── Audit columns ─────────────────────────────────────────
        .withColumn("_silver_processed_at", F.current_timestamp())
    )
    
    # Attach tracking hashes to the enriched output
    return add_audit_columns(enriched_stream, "SILVER", "fraud_engine_v1")

# ============================================================
# SCD TYPE 2 — Customer Dimension (Native DLT)
# ============================================================

dlt.create_streaming_table(
    name    = "silver.silver_customers_scd2",
    comment = "Customer dimension — Native DLT SCD Type 2",
    table_properties = {
        "quality":                    "silver",
        "delta.enableChangeDataFeed": "true",
    }
)

dlt.apply_changes(
    target = "silver.silver_customers_scd2",
    source = "bronze.bronze_customers_raw",
    keys = ["customer_id"],
    sequence_by = F.col("_ingested_at"),
    stored_as_scd_type = 2,
    track_history_column_list = [
        "kyc_status", "account_status", "risk_score",
        "city", "state", "income_band", "is_politically_exposed"
    ]
)


# ============================================================
# SCD TYPE 1 — Merchant Dimension
# ============================================================

dlt.create_streaming_table(
    name    = "silver.silver_merchants_scd1",
    comment = "Merchant dimension — SCD Type 1 (current state only)",
    table_properties = {"quality": "silver"}
)

dlt.create_auto_cdc_flow(
    name             = "silver_merchants_cdc_flow",
    source           = "bronze.bronze_merchants_raw",
    target           = "silver.silver_merchants_scd1",
    keys             = ["merchant_id"],
    sequence_by      = F.col("_ingested_at"),
    apply_as_deletes = None,
    ignore_null_updates = True,
)
