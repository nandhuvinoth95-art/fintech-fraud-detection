# pipelines/data_quality.py
# ============================================================
# DATA QUALITY FRAMEWORK — Enterprise-grade validation
# ============================================================

import dlt
from pyspark.sql import functions as F

# ============================================================
# EXPECTATION SUITE (To be used with @dlt.expect_all)
# ============================================================
"""
DATA QUALITY PHILOSOPHY FOR FINTECH:
1. BRONZE: Accept everything, quarantine nothing. (TRACK)
2. SILVER: Enforce business rules, drop corrupt records. (DROP)
3. GOLD: Fail pipeline on quality breach. (FAIL)
"""

BRONZE_EXPECTATIONS = {
    "transaction_id_present": "transaction_id IS NOT NULL",
    "amount_positive":        "amount > 0",
    "timestamp_present":      "timestamp IS NOT NULL",
    "customer_id_present":    "customer_id IS NOT NULL",
}

# Silver expectations (drop on violation)
SILVER_EXPECTATIONS_DROP = {
    "valid_amount_range":     "amount BETWEEN 0.01 AND 10000000",
    
    # Safely handle mixed-case string generation
    "valid_currency":         "UPPER(currency) IN ('INR','USD','EUR','GBP','AED','SGD','HKD')",
    "valid_payment_method":   "UPPER(payment_method) IN ('UPI','CREDIT_CARD','DEBIT_CARD','NETBANKING','WALLET','BNPL')",
    "valid_status":           "UPPER(transaction_status) IN ('SUCCESS','FAILED','PENDING','REVERSED')",
    "valid_device_type":      "UPPER(device_type) IN ('MOBILE','DESKTOP','POS','ATM','TABLET')",
    
    "valid_fraud_score":      "fraud_score BETWEEN 0.0 AND 1.0",
    
    # Expanded buffer to easily clear the 5.5 hour IST timezone difference
    "no_future_timestamp":    "event_timestamp <= CURRENT_TIMESTAMP() + INTERVAL 6 HOURS",
}

GOLD_EXPECTATIONS_FAIL = {
    "no_negative_fraud_score":  "fraud_score >= 0.0",
    "valid_risk_tier":          "customer_risk_tier IN ('LOW','MEDIUM','HIGH','CRITICAL')",
    "composite_score_bounded":  "composite_risk_score BETWEEN 0.0 AND 1.0",
}


# ============================================================
# AUDIT COLUMN STANDARD
# ============================================================

def add_audit_columns(df, layer: str, pipeline_name: str):
    """
    Adds highly optimized audit tracking columns.
    Uses xxhash64 for extreme performance on streaming datasets.
    """
    return (
        df
        .withColumn("_layer",           F.lit(layer))
        .withColumn("_pipeline_name",   F.lit(pipeline_name))
        .withColumn("_processed_at",    F.current_timestamp())
        .withColumn("_processed_date",  F.current_date())
        # xxhash64 operates on raw binary, avoiding JSON serialization overhead
        .withColumn("_record_hash",     F.xxhash64(*[F.col(c) for c in df.columns]))
    )


# ============================================================
# DUPLICATE DETECTION (Streaming-Safe)
# ============================================================

def detect_and_remove_duplicates(df, key_cols: list, watermark_col: str = None):
    """
    Streaming-safe deduplication. 
    
    NOTE: If your table requires keeping the *latest* update of a record, 
    do NOT use this function. Use dlt.apply_changes() (CDC) instead.
    This function is strictly for dropping exact duplicates in append-only streams.
    """
    
    # If it's a stream with a watermark, use the modern Spark 3.2+ stateful dedup
    if watermark_col:
        return df.dropDuplicatesWithinWatermark(key_cols)
    
    # Fallback for standard streaming/batch deduplication (keeps first record seen)
    return df.dropDuplicates(key_cols) 