import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, coalesce
from pyspark.sql.functions import col, from_json, expr, broadcast, to_timestamp
from pyspark.sql.types import *

# --- 1. Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Creates a Spark session optimized for Delta Lake and Kafka."""
    return SparkSession.builder \
        .appName("ATM_Unified_RealTime_DataMart_Prod") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.LocalLogStore") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def main():
    try:
        spark = create_spark_session()
        logger.info("Unified Spark Session Started. Connecting to Kafka...")

        # --- 2. Full Ledger Schema Definition ---
        payload_schema = StructType([
            StructField("LEDGER_ENTRY_ID", StringType(), False),
            StructField("TRANSACTION_ID", StringType(), False),
            StructField("ENTRY_SEQUENCE_NO", IntegerType(), False),
            StructField("TRANSACTION_TIMESTAMP", StringType(), False),
            StructField("PROCESSING_TIMESTAMP", StringType(), False),
            StructField("VALUE_DATE", StringType(), False),
            StructField("ACCOUNT_ID", LongType(), False),
            StructField("GL_ACCOUNT_CODE", StringType(), False),
            StructField("AMOUNT", DoubleType(), False),
            StructField("CURRENCY_CODE", StringType(), False),
            StructField("ENTRY_TYPE", StringType(), False),
            StructField("EQUIVALENT_BASE_AMOUNT", DoubleType(), False),
            StructField("FX_RATE", DoubleType(), False),
            StructField("TRANSACTION_TYPE_CODE", StringType(), False),
            StructField("CHANNEL_CODE", StringType(), False),
            StructField("PROCESSING_SYSTEM_CODE", StringType(), False),
            StructField("TRANSACTION_STATUS_CODE", StringType(), False),
            StructField("ENTRY_DESCRIPTION", StringType(), True),
            StructField("CREATED_DATE", StringType(), False),
            StructField("LAST_UPDATED_DATE", StringType(), False)
        ])

        envelope_schema = StructType([StructField("payload", payload_schema)])

        # --- 3. Kafka Source ---
        raw_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "postgres_cdc_.core_banking.financial_ledger") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

        # --- 4. In-Memory Parsing & Filtering ---
        parsed_df = raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
            .withColumn("data", from_json(col("json_str"), envelope_schema)) \
            .select("data.payload.*") \
            .filter("TRANSACTION_TYPE_CODE = '001'") \
            .filter("TRANSACTION_TYPE_CODE = '001'") \
            .withColumn("transaction_timestamp", to_timestamp("TRANSACTION_TIMESTAMP")) \
            .withWatermark("transaction_timestamp", "10 minutes")
            #.withColumn("event_time", 
                #coalesce(
                 #   to_timestamp("TRANSACTION_TIMESTAMP", "yyyy-MM-dd HH:mm:ss"),
                  #  to_timestamp("TRANSACTION_TIMESTAMP", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
                   # to_timestamp("TRANSACTION_TIMESTAMP") # Default fallback
                #)
            #) \
            #.filter(col("event_time").isNotNull()) \
            #.withWatermark("event_time", "1 minutes")

        # --- 5. Stream-to-Stream Join ---
        debit_df = parsed_df.filter("ENTRY_TYPE = 'DR'") \
            .withColumnRenamed("ACCOUNT_ID", "customer_account_id") \
            .alias("dr")

        credit_df = parsed_df.filter("ENTRY_TYPE = 'CR'") \
            .withColumnRenamed("ACCOUNT_ID", "atm_account_id") \
            .alias("cr")

        # We use a specific alias for codes here to avoid collision later
        txn_matched_df = debit_df.join(
            credit_df,
            expr("""
                dr.TRANSACTION_ID = cr.TRANSACTION_ID AND
                dr.transaction_timestamp BETWEEN 
                cr.transaction_timestamp - interval 10 minutes AND 
                cr.transaction_timestamp + interval 10 minutes
            """)
        ).select(
            col("dr.TRANSACTION_ID").alias("transaction_id"),
            col("dr.transaction_timestamp"),
            "customer_account_id",
            "atm_account_id",
            col("dr.AMOUNT").alias("withdrawal_amount"),
            col("dr.TRANSACTION_TYPE_CODE").alias("join_type_code"),
            col("dr.CHANNEL_CODE").alias("join_channel_code")
        )

        # --- 6. Dimension Enrichment (REFIXED FOR AMBIGUITY) ---
        logger.info("Enriching with Silver dimension tables...")
        
        atms_df = spark.read.format("delta").load("/delta/silver/atms")
        branches_df = spark.read.format("delta").load("/delta/silver/branches")
        
        # RENAME columns in reference tables to ensure they don't collide with the stream
        ref_types_df = spark.read.format("delta").load("/delta/silver/ref_transaction_types") \
            .withColumnRenamed("type_code", "ref_type_code")
            
        ref_ch_df = spark.read.format("delta").load("/delta/silver/ref_channels") \
            .withColumnRenamed("channel_code", "ref_channel_code")

        final_df = txn_matched_df \
            .join(broadcast(atms_df), txn_matched_df["atm_account_id"] == atms_df["cash_gl_account_id"], "left") \
            .join(broadcast(branches_df), "branch_id", "left") \
            .join(broadcast(ref_types_df), txn_matched_df["join_type_code"] == ref_types_df["ref_type_code"], "left") \
            .join(broadcast(ref_ch_df), txn_matched_df["join_channel_code"] == ref_ch_df["ref_channel_code"], "left") \
            .select(
                "transaction_id", 
                "transaction_timestamp", 
                "customer_account_id", 
                "atm_account_id", 
                "atm_id", 
                "location_name", 
                "branch_id", 
                "branch_name", 
                col("join_type_code").alias("type_code"), 
                col("type_name").alias("type_name"), 
                col("join_channel_code").alias("channel_code"), 
                col("channel_name").alias("channel_name"), 
                "withdrawal_amount"
            )

        # --- 7. Sink ---
        logger.info("Starting Streaming Query to Data Mart...")
        
        # Add this temporary query to your script to see data in the terminal logs
        debug_query = final_df.writeStream \
                .format("console") \
                .outputMode("append") \
                .trigger(processingTime='10 seconds') \
                .start()
        
        # Use v2 for checkpoint and mart to ensure a clean start
        query = final_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "/delta/checkpoints/atm_unified_prod") \
            .trigger(processingTime='10 seconds') \
            .start("/delta/marts/atm_txn_data_mart_v2")

        query.awaitTermination()

    except Exception as e:
        logger.error(f"FATAL ERROR in Unified Pipeline: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()