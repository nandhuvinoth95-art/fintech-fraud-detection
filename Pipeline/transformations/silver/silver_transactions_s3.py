# --- ENTERPRISE DATA QUALITY FRAMEWORK ---
import sys
import os

# Tell Python to look in the transformations/ folder
sys.path.append(os.path.abspath('/Workspace/Repos/nandhuvinoth95@gmail.com/fintech-fraud-detection/Pipeline/transformations'))

import dlt
from pyspark.sql import functions as F

from data_quality.data_quality import SILVER_EXPECTATIONS_DROP, add_audit_columns

@dlt.table(
    name="silver.silver_transactions_s3_cleaned",  
    comment="Cleaned and deduplicated transactions from S3 batch history",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_all_or_drop(SILVER_EXPECTATIONS_DROP)  # <--- DATA QUALITY GATE ADDED
def silver_transactions_s3_cleaned():
    raw_s3_stream = dlt.read_stream("bronze.bronze_transactions_s3")
    
    # Apply standard cleaning and enrichment logic
    cleaned = (
        raw_s3_stream
        .dropDuplicates(["transaction_id"]) # Basic deduplication for batch files
        .withColumn("amount_usd", F.col("amount")) # Add FX logic if needed
        .withColumn("event_timestamp", F.to_timestamp("timestamp"))
        .withColumn("risk_band", 
            F.when(F.col("fraud_score") >= 0.8, "CRITICAL")
             .otherwise("LOW")
        )
        .withColumn("_processing_path", F.lit("S3_BATCH")) # Track where it came from
    )
    
    # <--- AUDIT COLUMNS INJECTED HERE
    return add_audit_columns(cleaned, "SILVER", "fraud_engine_v1")


@dlt.table(
    name="silver.silver_transactions_unified",      
    comment="Unified table of both real-time Kafka and batch S3 transactions",
    table_properties={"quality": "silver"}
)
def silver_transactions_unified():
    live_txns = dlt.read("silver.silver_transactions_cleaned")
    batch_txns = dlt.read("silver.silver_transactions_s3_cleaned")
    
    # unionByName with allowMissingColumns prevents crashes if one source has an extra column
    return live_txns.unionByName(batch_txns, allowMissingColumns=True)