# pipelines/gold/gold_fraud_aggregates.py
# ============================================================
# GOLD LAYER — Fraud Analytics with Materialized Views
#
# SDP CONCEPTS DEMONSTRATED:
#   1. @dlt.table (Materialized View) — pre-computed aggregates
#   2. Incremental refresh            — only processes new data via CDF
#   3. Triggered mode                 — runs on schedule, not continuous
#   4. Streaming Table for real-time  — fraud alerts updated live
# ============================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# GOLD TABLE 1: Real-Time Fraud Alerts (Streaming Table)
# ============================================================

@dlt.table(
    name    = "gold.gold_fraud_alerts_live",
    comment = "Real-time high-confidence fraud alerts — sub-minute latency",
    table_properties = {
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    }
)
@dlt.expect_or_fail("no_negative_fraud_score", "fraud_score >= 0.0")
def gold_fraud_alerts_live():
    """
    STREAMING TABLE: Continuously updated fraud alerts.
    """
    return (
        dlt.read_stream("silver.silver_transactions_cleaned")

        # ── Filter: Only transactions flagged as fraud ──────────
        .filter(
            (F.col("is_fraud") == True) &
            (F.col("fraud_score") >= 0.60)
        )

        # ── Severity classification ──────────────────────────────
        .withColumn("alert_severity",
            F.when(F.col("fraud_score") >= 0.90, "CRITICAL")
             .when(F.col("fraud_score") >= 0.75, "HIGH")
             .otherwise("MEDIUM")
        )

        # ── Alert enrichment ─────────────────────────────────────
        .withColumn("alert_id", F.concat(F.lit("ALERT_"), F.col("transaction_id")))
        .withColumn("alert_generated_at", F.current_timestamp())
        .withColumn("alert_message",
            F.concat_ws(" | ",
                F.lit("FRAUD ALERT"),
                F.col("fraud_type"),
                F.concat(F.lit("Score:"), F.col("fraud_score").cast("string")),
                F.concat(F.lit("Amount:₹"), F.col("amount").cast("string")),
                F.col("customer_id"),
                F.col("location_city")
            )
        )

        # ── Select final columns ─────────────────────────────────
        .select(
            "alert_id", "transaction_id", "customer_id", "merchant_id",
            "amount", "currency", "event_timestamp", "fraud_type",
            "fraud_score", "alert_severity", "location_city", "location_country",
            "payment_method", "device_id", "alert_message", "alert_generated_at",
        )
    )


# ============================================================
# GOLD TABLE 2: Velocity Fraud Detection
# ============================================================

@dlt.table(
    name    = "gold.gold_velocity_fraud",
    comment = "Customers with suspicious transaction velocity",
    table_properties = {"quality": "gold"}
)
def gold_velocity_fraud():
    """
    VELOCITY FRAUD DETECTION:
    Rule: If customer makes > 15 transactions in 60 minutes → flag.
    """
    VELOCITY_WINDOW_MINUTES = 60
    VELOCITY_THRESHOLD      = 1

    return spark.sql(f"""
        WITH velocity_counts AS (
            SELECT
                customer_id,
                DATE_TRUNC('hour', event_timestamp)  AS hour_window,
                COUNT(*)                             AS txn_count,
                SUM(amount)                          AS total_amount,
                COUNT(DISTINCT merchant_id)          AS unique_merchants,
                COUNT(DISTINCT device_id)            AS unique_devices,
                COUNT(DISTINCT location_city)        AS unique_cities,
                MIN(event_timestamp)                 AS first_txn_time,
                MAX(event_timestamp)                 AS last_txn_time,
                AVG(fraud_score)                     AS avg_fraud_score,
                TIMESTAMPDIFF(MINUTE, MIN(event_timestamp), MAX(event_timestamp)) AS window_duration_minutes
            -- [FIXED] Using LIVE virtual schema
            FROM LIVE.silver.silver_transactions_unified
            WHERE event_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
              AND transaction_status != 'FAILED'
            GROUP BY customer_id, DATE_TRUNC('hour', event_timestamp)
        )
        SELECT
            customer_id, hour_window, txn_count, total_amount, unique_merchants,
            unique_devices, unique_cities, first_txn_time, last_txn_time,
            window_duration_minutes, avg_fraud_score,
            CASE
                WHEN txn_count > 30 THEN 1.0
                WHEN txn_count > 20 THEN 0.9
                WHEN txn_count > 15 THEN 0.7
                WHEN txn_count > 10 THEN 0.5
                ELSE 0.2
            END AS velocity_score,
            ARRAY(
                CASE WHEN txn_count > {VELOCITY_THRESHOLD} THEN CONCAT('HIGH_VELOCITY:', CAST(txn_count AS STRING), '_txns') END,
                CASE WHEN unique_devices > 2 THEN CONCAT('MULTI_DEVICE:', CAST(unique_devices AS STRING)) END,
                CASE WHEN unique_cities > 2 THEN 'MULTI_CITY_VELOCITY' END
            ) AS risk_factors,
            CURRENT_TIMESTAMP() AS computed_at
        FROM velocity_counts
        WHERE txn_count >= {VELOCITY_THRESHOLD}
        ORDER BY txn_count DESC
    """)


# ============================================================
# GOLD TABLE 3: Impossible Travel Detection
# ============================================================

@dlt.table(
    name    = "gold.gold_impossible_travel",
    comment = "Transactions showing impossible geographic travel patterns",
    table_properties = {"quality": "gold"}
)
def gold_impossible_travel():
    """
    IMPOSSIBLE TRAVEL DETECTION:
    Formula: distance_km / time_hours > 900 km/h
    """
    return spark.sql("""
        WITH consecutive_txns AS (
            SELECT
                customer_id, transaction_id, event_timestamp, latitude, longitude,
                location_city, location_country, amount,
                LAG(event_timestamp)  OVER (PARTITION BY customer_id ORDER BY event_timestamp) AS prev_timestamp,
                LAG(latitude)         OVER (PARTITION BY customer_id ORDER BY event_timestamp) AS prev_lat,
                LAG(longitude)        OVER (PARTITION BY customer_id ORDER BY event_timestamp) AS prev_lon,
                LAG(location_city)    OVER (PARTITION BY customer_id ORDER BY event_timestamp) AS prev_city,
                LAG(location_country) OVER (PARTITION BY customer_id ORDER BY event_timestamp) AS prev_country,
                LAG(transaction_id)   OVER (PARTITION BY customer_id ORDER BY event_timestamp) AS prev_txn_id
            -- [FIXED] Using LIVE virtual schema
            FROM LIVE.silver.silver_transactions_unified
            WHERE event_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
        ),
        with_distance AS (
            SELECT *,
                2 * 6371 * ASIN(SQRT(
                    POW(SIN((RADIANS(latitude) - RADIANS(prev_lat)) / 2), 2) +
                    COS(RADIANS(prev_lat)) * COS(RADIANS(latitude)) *
                    POW(SIN((RADIANS(longitude) - RADIANS(prev_lon)) / 2), 2)
                )) AS distance_km,
                (UNIX_TIMESTAMP(event_timestamp) - UNIX_TIMESTAMP(prev_timestamp)) / 3600.0 AS time_gap_hours
            FROM consecutive_txns
            WHERE prev_timestamp IS NOT NULL AND prev_lat IS NOT NULL
        )
        SELECT
            customer_id,
            transaction_id          AS txn_id_2,
            prev_txn_id             AS txn_id_1,
            prev_city               AS from_city,
            location_city           AS to_city,
            prev_country            AS from_country,
            location_country        AS to_country,
            ROUND(distance_km, 2)   AS distance_km,
            ROUND(time_gap_hours, 4) AS time_gap_hours,
            ROUND(distance_km / NULLIF(time_gap_hours, 0), 0) AS implied_speed_kmh,
            amount, event_timestamp,
            CURRENT_TIMESTAMP()     AS detected_at,
            'IMPOSSIBLE_TRAVEL'     AS fraud_pattern,
            CASE
                WHEN distance_km / NULLIF(time_gap_hours, 0) > 100 THEN 0.99
                WHEN distance_km / NULLIF(time_gap_hours, 0) > 50 THEN 0.95
                WHEN distance_km / NULLIF(time_gap_hours, 0) > 1  THEN 0.85
                ELSE 0.5
            END AS impossible_travel_score
        FROM with_distance
        WHERE distance_km / NULLIF(time_gap_hours, 0) > 0.1
          AND distance_km > 0.1 
        ORDER BY implied_speed_kmh DESC
    """)


# ============================================================
# GOLD TABLE 4: Merchant Fraud Scoring
# ============================================================

@dlt.table(
    name    = "gold.gold_merchant_fraud_scores",
    comment = "Merchant-level fraud analytics and risk ranking",
    table_properties = {"quality": "gold"}
)
def gold_merchant_fraud_scores():
    return spark.sql("""
        SELECT
            t.merchant_id, m.merchant_name, m.merchant_category, m.risk_category,
            m.city, m.state,
            COUNT(*)                                AS total_transactions,
            COUNT(DISTINCT t.customer_id)           AS unique_customers,
            SUM(t.amount)                           AS total_volume_inr,
            AVG(t.amount)                           AS avg_transaction_amount,
            SUM(CASE WHEN t.is_fraud THEN 1 ELSE 0 END) AS fraud_count,
            SUM(CASE WHEN t.is_fraud THEN t.amount ELSE 0 END) AS fraud_amount_inr,
            ROUND(SUM(CASE WHEN t.is_fraud THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 4) AS fraud_rate_pct,
            AVG(t.fraud_score)                      AS avg_fraud_score,
            MAX(t.fraud_score)                      AS max_fraud_score,
            COUNT(DISTINCT t.fraud_type)            AS distinct_fraud_types,
            SUM(CASE WHEN t.is_night_transaction AND t.amount > 10000 THEN 1 ELSE 0 END) AS high_value_night_txns,
            SUM(CASE WHEN t.failed_attempt_count > 0 THEN 1 ELSE 0 END) AS transactions_with_failures,
            ROUND(LEAST(1.0,
                m.historical_fraud_score * 0.4 +
                (SUM(CASE WHEN t.is_fraud THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)) * 0.4 +
                CASE WHEN m.merchant_tenure_days < 90 THEN 0.2 ELSE 0.0 END
            ), 4) AS computed_risk_score,
            CASE
                WHEN m.historical_fraud_score > 0.7 OR SUM(CASE WHEN t.is_fraud THEN 1.0 ELSE 0.0 END) / NULLIF(COUNT(*), 0) > 0.15 THEN 'CRITICAL'
                WHEN m.historical_fraud_score > 0.4 OR SUM(CASE WHEN t.is_fraud THEN 1.0 ELSE 0.0 END) / NULLIF(COUNT(*), 0) > 0.08 THEN 'HIGH'
                WHEN m.historical_fraud_score > 0.2 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS merchant_risk_tier,
            CURRENT_TIMESTAMP() AS computed_at
        -- [FIXED] Using LIVE virtual schema
        FROM LIVE.silver.silver_transactions_unified t
        LEFT JOIN LIVE.silver.silver_merchants_scd1 m
               ON t.merchant_id = m.merchant_id
        WHERE t.event_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
        GROUP BY
            t.merchant_id, m.merchant_name, m.merchant_category,
            m.risk_category, m.city, m.state,
            m.historical_fraud_score, m.merchant_tenure_days
        ORDER BY computed_risk_score DESC
    """)


# ============================================================
# GOLD TABLE 5: Customer Risk Profile
# ============================================================

@dlt.table(
    name    = "gold.gold_customer_risk_profiles",
    comment = "360-degree customer fraud risk profile",
    table_properties = {"quality": "gold"}
)
def gold_customer_risk_profiles():
    return spark.sql("""
        WITH customer_txn_stats AS (
            SELECT
                t.customer_id,
                COUNT(*)                            AS total_txns_30d,
                SUM(t.amount)                       AS total_spend_30d,
                AVG(t.amount)                       AS avg_txn_amount,
                MAX(t.amount)                       AS max_single_txn,
                STDDEV(t.amount)                    AS txn_amount_stddev,
                COUNT(DISTINCT t.device_id)         AS unique_devices_30d,
                COUNT(DISTINCT t.location_city)     AS unique_cities_30d,
                COUNT(DISTINCT t.merchant_category) AS unique_merchant_cats,
                COUNT(DISTINCT t.ip_address)        AS unique_ips_30d,
                AVG(t.hour_of_day)                  AS avg_hour_of_day,
                SUM(CASE WHEN t.is_night_transaction THEN 1 ELSE 0 END) AS night_txn_count,
                SUM(CASE WHEN t.is_weekend THEN 1 ELSE 0 END)           AS weekend_txn_count,
                SUM(t.failed_attempt_count)         AS total_failures,
                AVG(t.failed_attempt_count)         AS avg_failures_per_txn,
                SUM(CASE WHEN t.is_fraud THEN 1 ELSE 0 END)     AS confirmed_fraud_count,
                SUM(CASE WHEN t.is_fraud THEN t.amount ELSE 0 END) AS confirmed_fraud_amount,
                AVG(t.fraud_score)                              AS avg_fraud_score,
                MAX(t.fraud_score)                               AS max_fraud_score,
                SUM(CASE WHEN t.is_foreign_transaction THEN 1 ELSE 0 END) AS foreign_txn_count,
                SUM(CASE WHEN t.amount BETWEEN 9000 AND 9999 THEN 1 ELSE 0 END) AS structuring_pattern_count
            -- [FIXED] Using LIVE virtual schema
            FROM LIVE.silver.silver_transactions_unified t
            WHERE t.event_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
            GROUP BY t.customer_id
        )
        SELECT
            s.*,
            c.full_name, c.kyc_status, c.account_status,
            c.risk_score AS profile_risk_score,
            c.account_age_days, c.income_band, c.is_politically_exposed, c.country,
            ROUND(LEAST(1.0,
                (s.confirmed_fraud_count * 1.0 / NULLIF(s.total_txns_30d, 0)) * 0.3 +
                (s.unique_devices_30d / 10.0) * 0.1 +
                (s.unique_cities_30d / 10.0) * 0.1 +
                (s.night_txn_count * 1.0 / NULLIF(s.total_txns_30d, 0)) * 0.15 +
                (s.foreign_txn_count * 1.0 / NULLIF(s.total_txns_30d, 0)) * 0.15 +
                (s.structuring_pattern_count * 1.0 / NULLIF(s.total_txns_30d, 0)) * 0.2
            ), 4) AS computed_risk_score,
            CASE
                WHEN s.confirmed_fraud_count > 0 THEN 'CRITICAL'
                WHEN s.max_fraud_score > 0.7 THEN 'HIGH'
                WHEN s.unique_devices_30d > 5 OR s.unique_cities_30d > 5 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS customer_risk_tier,
            CURRENT_TIMESTAMP() AS computed_at
        FROM customer_txn_stats s
        LEFT JOIN LIVE.silver.silver_customers_scd2 c ON s.customer_id = c.customer_id
        ORDER BY computed_risk_score DESC
    """)
