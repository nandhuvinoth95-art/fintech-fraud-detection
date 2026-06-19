# pipelines/advanced/monitoring_metrics.py
# ============================================================
# GOLD LAYER — Advanced Monitoring & Analytics
#
# SDP CONCEPTS DEMONSTRATED:
#   1. Pipeline Health Metrics         — DLT metadata queries
#   2. Anomaly Detection               — Statistical profiling
#   3. Real-time alerting integration  — SNS/Slack hooks
# ============================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# PIPELINE HEALTH METRICS (Gold Layer DQ Monitoring)
# ============================================================

@dlt.table(
    name="gold.pipeline_health_metrics",
    comment="Real-time monitoring of pipeline health and data quality metrics"
)
def pipeline_health_metrics():
    """
    Cross-layer monitoring: tracks data quality trends, volume, and
    processing lag across Bronze, Silver, and Gold layers.
    """
    
    # 1. Compute metrics for the Bronze layer ingestion
    bronze_metrics = (
        dlt.read("LIVE.bronze.bronze_transactions_raw")
        .select(
            F.lit("bronze_transactions").alias("layer_table"),
            F.col("_ingested_at")
        )
        .groupBy("layer_table")
        .agg(
            F.count("*").alias("row_count"),
            F.min("_ingested_at").alias("oldest_record"),
            F.max("_ingested_at").alias("newest_record")
        )
        # Fixed: F.timestamp_diff is not valid PySpark. Using SQL expr instead.
        .withColumn(
            "lag_minutes", 
            F.expr("TIMESTAMPDIFF(MINUTE, newest_record, current_timestamp())")
        )
    )

    # 2. Compute metrics for the Silver layer processing
    silver_metrics = (
        dlt.read("LIVE.silver.silver_transactions_cleaned")
        .select(
            F.lit("silver_transactions").alias("layer_table"),
            F.col("_processed_at")
        )
        .groupBy("layer_table")
        .agg(
            F.count("*").alias("row_count"),
            F.min("_processed_at").alias("oldest_record"),
            F.max("_processed_at").alias("newest_record")
        )
        .withColumn(
            "lag_minutes",
            F.expr("TIMESTAMPDIFF(MINUTE, newest_record, current_timestamp())")
        )
    )

    # 3. Compute metrics for the Gold layer alerts
    gold_metrics = (
        dlt.read("LIVE.gold.gold_fraud_alerts_live")
        .select(
            F.lit("gold_fraud_alerts").alias("layer_table"),
            F.col("alert_generated_at")
        )
        .groupBy("layer_table")
        .agg(
            F.count("*").alias("row_count"),
            F.min("alert_generated_at").alias("oldest_record"),
            F.max("alert_generated_at").alias("newest_record")
        )
        .withColumn(
            "lag_minutes",
            F.expr("TIMESTAMPDIFF(MINUTE, newest_record, current_timestamp())")
        )
    )

    # Combine all layer metrics
    return (
        bronze_metrics
        .unionByName(silver_metrics)
        .unionByName(gold_metrics)
        .withColumn("computed_at", F.current_timestamp())
        .withColumn("health_status",
            F.when(F.col("lag_minutes") > 60, "CRITICAL")
             .when(F.col("lag_minutes") > 30, "WARNING")
             .otherwise("HEALTHY")
        )
    )


# ============================================================
# ANOMALY DETECTION — Unusual Transaction Patterns
# ============================================================

@dlt.table(
    name="gold.anomaly_detection_stats",
    comment="Statistical anomaly detection on transaction volumes and amounts"
)
def anomaly_detection_stats():
    """
    Detects anomalies in transaction patterns using rolling statistics.
    Flags outliers based on Z-score > 3.
    """
    
    # Calculate rolling statistics over 7-day windows
    window_spec = Window.orderBy("transaction_date").rowsBetween(-6, 0)
    
    daily_stats = (
        dlt.read("LIVE.silver.silver_transactions_unified")
        .groupBy("transaction_date")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.countDistinct("customer_id").alias("unique_customers")
        )
        .withColumn("rolling_avg_txn_count",      F.avg("txn_count").over(window_spec))
        .withColumn("rolling_stddev_txn_count",   F.stddev("txn_count").over(window_spec))
        .withColumn("rolling_avg_total_amount",   F.avg("total_amount").over(window_spec))
        .withColumn("rolling_stddev_total_amount", F.stddev("total_amount").over(window_spec))
    )
    
    # Z-score calculation
    anomaly_flags = (
        daily_stats
        .withColumn("txn_count_z_score",
            (F.col("txn_count") - F.col("rolling_avg_txn_count")) / 
            F.coalesce(F.col("rolling_stddev_txn_count"), F.lit(1))
        )
        .withColumn("total_amount_z_score",
            (F.col("total_amount") - F.col("rolling_avg_total_amount")) / 
            F.coalesce(F.col("rolling_stddev_total_amount"), F.lit(1))
        )
        .withColumn("is_anomaly",
            (F.abs(F.col("txn_count_z_score")) > 3) | 
            (F.abs(F.col("total_amount_z_score")) > 3)
        )
        .withColumn("anomaly_severity",
            F.when(F.abs(F.col("txn_count_z_score")) > 4, "CRITICAL")
             .when(F.abs(F.col("txn_count_z_score")) > 3, "HIGH")
             .otherwise("NORMAL")
        )
        .withColumn("computed_at", F.current_timestamp())
    )
    
    return anomaly_flags


# ============================================================
# FRAUD PATTERN CLUSTERING (Simplified — No ML Model)
# ============================================================

@dlt.table(
    name="gold.fraud_pattern_clusters",
    comment="High-level fraud pattern segmentation based on transaction characteristics"
)
def fraud_pattern_clusters():
    """
    Creates fraud clusters based on transaction patterns WITHOUT using ML models.
    Uses heuristic-based segmentation.
    """
    
    return (
        dlt.read("LIVE.gold.gold_fraud_alerts_live")
        .withColumn("fraud_cluster",
            F.when(
                (F.col("fraud_type") == "VELOCITY") & (F.col("amount") < 1000),
                "MICRO_VELOCITY_FRAUD"
            )
            .when(
                (F.col("fraud_type") == "IMPOSSIBLE_TRAVEL"),
                "GEO_ANOMALY_FRAUD"
            )
            .when(
                (F.col("fraud_score") >= 0.90) & (F.col("amount") > 10000),
                "HIGH_VALUE_FRAUD"
            )
            .otherwise("GENERAL_FRAUD")
        )
        .groupBy("fraud_cluster", "fraud_type")
        .agg(
            F.count("*").alias("fraud_count"),
            F.sum("amount").alias("total_fraud_amount"),
            F.avg("fraud_score").alias("avg_fraud_score"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("merchant_id").alias("unique_merchants")
        )
        .withColumn("computed_at", F.current_timestamp())
    )
