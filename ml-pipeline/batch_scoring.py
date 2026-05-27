import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Hourly batch Spark script to compact Iceberg Parquet files and execute predictive health scoring
def main():
    spark = SparkSession.builder \
        .appName("IcebergSmallFileCompactorAndScoring") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.catalog.type", "hadoop") \
        .config("spark.sql.catalog.catalog.warehouse", "s3a://telemetry-warehouse/") \
        .getOrCreate()
        
    print("Spark Session loaded. Connected to Iceberg catalogs.")

    # 1. Compact small files (commits every 60s generated metadata clusters)
    # Merges fragments into massive optimal Parquet blocks
    print("Executing hourly Parquet file compactors via write rewrite API...")
    spark.sql("CALL catalog.system.rewrite_data_files(table => 'db.telemetry_raw', options => map('max-file-size-bytes', '536870912'))")
    print("Compaction complete.")

    # 2. Predictive Maintenance Engine
    df = spark.read.table("catalog.db.telemetry_raw")
    # Score vibration / pressure features for mechanical failures
    scored_df = df.withColumn(
        "predictive_wear_score",
        when((col("metric_name") == "vibration") & (col("value") > 3.0), 0.85)
        .when((col("metric_name") == "vibration") & (col("value") > 4.2), 0.98)
        .otherwise(0.05)
    )
    
    scored_df.write.mode("append").saveAsTable("catalog.db.ml_scores")
    print("Hourly Batch ML Predictive maintenance scoring updated successfully.")

if __name__ == "__main__":
    main()
