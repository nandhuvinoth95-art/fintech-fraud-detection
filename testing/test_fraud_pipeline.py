
import pytest
import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# ------------------------------------------------------------
# THE FIX: Retrieve the live session injected by the notebook
# ------------------------------------------------------------
@pytest.fixture(scope="session")
def spark_session():
    return pytest.spark 

# ============================================================
# [KEEP ALL YOUR TEST CLASSES EXACTLY THE SAME BELOW THIS]
# (TestDataGeneration, TestBronzeLayer, TestSCD2, TestFraudRules)
# ============================================================


# ============================================================
# TEST: Data Generation
# ============================================================
class TestDataGeneration:
    def test_customer_generation(self):
        import sys
        # FIXED: Path should only point to the folder, not the .py file
        sys.path.append("/Workspace/Repos/nandhuvinoth95@gmail.com/fintech-fraud-detection/data_generation") 
        try:
            from generate_transactions import CustomerGenerator
        except ImportError:
            pytest.skip("Skipping test: generate_transactions module not found in path.")

        gen       = CustomerGenerator(num_customers=100)
        customers = gen.generate()

        assert len(customers) == 100
        fraud_customers = [c for c in customers if c.is_synthetic_fraud_customer]
        assert len(fraud_customers) == 20   

        for c in customers:
            assert c.customer_id.startswith("CUST_")
            assert 0.0 <= c.risk_score <= 1.0
            assert c.kyc_status in ["VERIFIED", "PENDING", "FAILED"]

    def test_transaction_fraud_rate(self):
        import sys
        sys.path.append("/Workspace/Repos/nandhuvinoth95@gmail.com/fintech-fraud-detection/data_generation") 
        try:
            from generate_transactions import CustomerGenerator, MerchantGenerator, TransactionGenerator
        except ImportError:
            pytest.skip("Skipping test: generate_transactions module not found in path.")
            
        customers  = CustomerGenerator(200).generate()
        merchants  = MerchantGenerator(50).generate()
        gen        = TransactionGenerator(customers, merchants, fraud_rate=0.08)
        txns       = gen.generate(num_transactions=1000)

        fraud_count = sum(1 for t in txns if t.is_fraud)
        fraud_rate  = fraud_count / len(txns)

        assert 0.03 <= fraud_rate <= 0.20

    def test_impossible_travel_logic(self):
        import sys
        sys.path.append("/Workspace/Repos/nandhuvinoth95@gmail.com/fintech-fraud-detection/data_generation") 
        try:
            from generate_transactions import CustomerGenerator, MerchantGenerator, TransactionGenerator
        except ImportError:
            pytest.skip("Skipping test: generate_transactions module not found in path.")
            
        customers = CustomerGenerator(50).generate()
        merchants = MerchantGenerator(20).generate()
        gen       = TransactionGenerator(customers, merchants)
        
        customers[0].is_synthetic_fraud_customer = True
        customers[0].fraud_type = "IMPOSSIBLE_TRAVEL"

        travel_txns = gen._inject_impossible_travel(customers[0], datetime.now())

        assert len(travel_txns) == 2
        assert travel_txns[0].is_fraud == False   
        assert travel_txns[1].is_fraud == True    
        assert travel_txns[1].fraud_type == "IMPOSSIBLE_TRAVEL"

# ============================================================
# [KEEP ALL YOUR OTHER TEST CLASSES THE SAME DOWN HERE]
# (TestBronzeLayer, TestSCD2, TestFraudRules)
# ============================================================


# ============================================================
# TEST: Bronze Layer Schema & Expectations
# ============================================================
class TestBronzeLayer:
    # Notice we pass 'spark_session' here which uses our fixture above
    def test_transaction_schema_validation(self, spark_session):
        valid_data = [{
            "transaction_id": "TXN_ABC123",
            "customer_id":    "CUST_001",
            "merchant_id":    "MERCH_001",
            "amount":         1500.00,
            "currency":       "INR",
            "timestamp":      datetime.now().isoformat(),
        }]
        df_valid = spark_session.createDataFrame(valid_data)
        assert df_valid.filter(F.col("amount") > 0).count() == 1

        invalid_data = [{"transaction_id": "TXN_BAD", "amount": -100.0}]
        df_invalid = spark_session.createDataFrame(invalid_data)
        df_filtered = df_invalid.filter(F.col("amount") > 0)
        
        assert df_filtered.count() == 0   

    def test_kafka_schema_parsing(self, spark_session):
        kafka_data = [
            ('{"transaction_id":"TXN_001","amount":1000.0}',),
            ('MALFORMED_JSON{{{',),
            ('{"transaction_id":"TXN_002","amount":2000.0}',),
        ]
        df = spark_session.createDataFrame(kafka_data, ["value"])

        parsed = df.select(
            F.from_json(F.col("value"), "transaction_id STRING, amount DOUBLE").alias("d")
        ).select("d.*")

        valid_count = parsed.filter(F.col("transaction_id").isNotNull()).count()
        assert valid_count == 2


# ============================================================
# TEST: SCD Type 2 Logic
# ============================================================
class TestSCD2:
    def test_scd2_creates_new_version_on_change(self, spark_session):
        existing = spark_session.createDataFrame([{
            "customer_id": "CUST_001",
            "kyc_status":  "PENDING",
            "risk_score":  0.3,
        }])

        updated = spark_session.createDataFrame([{
            "customer_id": "CUST_001",
            "kyc_status":  "VERIFIED",   
            "risk_score":  0.1,          
        }])

        old_hash = existing.select(F.xxhash64("kyc_status", "risk_score")).collect()[0][0]
        new_hash = updated.select(F.xxhash64("kyc_status", "risk_score")).collect()[0][0]

        assert old_hash != new_hash

    def test_scd2_no_change_no_new_version(self, spark_session):
        existing = spark_session.createDataFrame([{
            "customer_id": "CUST_002",
            "kyc_status":  "VERIFIED",
            "risk_score":  0.2,
        }])
        updated = spark_session.createDataFrame([{
            "customer_id": "CUST_002",
            "kyc_status":  "VERIFIED",   
            "risk_score":  0.2,          
        }])

        old_hash = existing.select(F.xxhash64("kyc_status", "risk_score")).collect()[0][0]
        new_hash = updated.select(F.xxhash64("kyc_status", "risk_score")).collect()[0][0]

        assert old_hash == new_hash


# ============================================================
# TEST: Fraud Detection Rules (Gold Layer)
# ============================================================
class TestFraudRules:
    def test_velocity_detection(self, spark_session):
        base_ts = datetime.now()
        customer_id = "CUST_VELOCITY"

        txn_data = []
        for i in range(25):
            txn_data.append({
                "transaction_id": f"TXN_{i:04d}",
                "customer_id":    customer_id,
                "amount":         100.0,
                "event_timestamp": (base_ts + timedelta(minutes=i)).isoformat(),
                "transaction_status": "SUCCESS",
            })

        df = spark_session.createDataFrame(txn_data)
        df = df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

        velocity = (
            df.groupBy(
                "customer_id",
                F.window("event_timestamp", "60 minutes")
            )
            .agg(F.count("*").alias("txn_count"))
            .filter(F.col("txn_count") > 15)
        )

        assert velocity.count() > 0
        max_count = velocity.select(F.max("txn_count")).collect()[0][0]
        assert max_count >= 15

    def test_aml_structuring_detection(self, spark_session):
        txn_data = [
            {"customer_id": "CUST_AML", "amount": 9100.0, "merchant_category": "MONEY_TRANSFER"},
            {"customer_id": "CUST_AML", "amount": 9500.0, "merchant_category": "MONEY_TRANSFER"},
            {"customer_id": "CUST_AML", "amount": 9800.0, "merchant_category": "MONEY_TRANSFER"},
            {"customer_id": "CUST_AML", "amount": 9200.0, "merchant_category": "CRYPTO_EXCHANGE"},
            {"customer_id": "CUST_GOOD", "amount": 500.0,  "merchant_category": "GROCERY"},
        ]
        df = spark_session.createDataFrame(txn_data)

        structuring = (
            df.filter(F.col("amount").between(9000, 9999))
              .groupBy("customer_id")
              .agg(F.count("*").alias("structuring_count"))
              .filter(F.col("structuring_count") >= 3)
        )

        assert structuring.count() == 1
        result = structuring.collect()[0]
        assert result["customer_id"] == "CUST_AML"
        assert result["structuring_count"] == 4

    def test_impossible_travel_speed_calculation(self, spark_session):
        txn_data = [
            {
                "customer_id":    "CUST_TRAVEL",
                "event_timestamp": "2024-01-15 22:00:00",
                "latitude":       19.076,   
                "longitude":      72.878,
            },
            {
                "customer_id":    "CUST_TRAVEL",
                "event_timestamp": "2024-01-15 22:20:00",
                "latitude":       51.507,   
                "longitude":      -0.128,
            }
        ]
        df = spark_session.createDataFrame(txn_data)
        df = df.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

        window_spec = Window.partitionBy("customer_id").orderBy("event_timestamp")
        
        df_with_lag = (
            df.withColumn("prev_lat", F.lag("latitude").over(window_spec))
              .withColumn("prev_lon", F.lag("longitude").over(window_spec))
              .withColumn("prev_ts", F.lag("event_timestamp").over(window_spec))
              .dropna()
        )

        result = df_with_lag.withColumn(
            "distance_km",
            2 * 6371 * F.asin(F.sqrt(
                F.pow(F.sin((F.radians("latitude") - F.radians("prev_lat")) / 2), 2) +
                F.cos(F.radians("prev_lat")) * F.cos(F.radians("latitude")) *
                F.pow(F.sin((F.radians("longitude") - F.radians("prev_lon")) / 2), 2)
            ))
        ).collect()

        distance = result[0]["distance_km"]
        assert distance > 5000


# ------------------------------------------------------------
# TWEAK 3: The Notebook Execution Trigger
# ------------------------------------------------------------
# This tells the notebook to run the tests and print the results 
# to the cell output.
retcode = pytest.main(["-v", "-p", "no:cacheprovider"])
