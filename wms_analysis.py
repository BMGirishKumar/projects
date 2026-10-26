import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark import StorageLevel

def run_wms_bottleneck_pipeline(data_input_path: str, output_report_path: str):
    """
    Executes a high-throughput WMS event processing pipeline to isolate warehouse bottleneck zones.
    """
    # SYSTEM FIX: Bypass Hadoop's native local file security check entirely by forcing Java's generic provider
    spark = SparkSession.builder \
        .appName("WMS_Bottleneck_analysis") \
        .master("local[*]") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    print("Spark Session started successfully")

    try:
        wms_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("timestamp", TimestampType(), True),
            StructField("item_id", StringType(), True),
            StructField("zone_id", StringType(), True),
            StructField("event_type", StringType(), True),           
            StructField("processing_time_sec", DoubleType(), True),  
            StructField("operator_id", StringType(), True)
        ])
        
        df_raw = spark.read \
            .format("json") \
            .schema(wms_schema) \
            .load(data_input_path)
            
        df_clean = df_raw.na.drop(subset=["event_id", "zone_id", "event_type"])

        df_transformed = df_clean \
            .withColumn("is_delayed", F.when(F.col("processing_time_sec") > 300, 1).otherwise(0)) \
            .withColumn("hour_of_day", F.hour(F.col("timestamp")))
            
        zone_meta_data = [
            ("ZONE_A", "High-Value Electronics", "Building 1"),
            ("ZONE_B", "Bulk Apparel", "Building 1"),
            ("ZONE_C", "Perishables and Cold Storage", "Building 2"),
            ("ZONE_D", "Hazmat", "Building 2")
        ]
        zone_columns = ["zone_id", "zone_name", "facility_location"]
        
        df_zone_lookup = spark.createDataFrame(zone_meta_data, schema=zone_columns)
        
        df_enriched = df_transformed.join(
            F.broadcast(df_zone_lookup),
            on="zone_id",
            how="inner"
        )
        
        df_bottlenecks = df_enriched.groupBy("facility_location", "zone_id", "zone_name") \
            .agg(
                F.count("event_id").alias("total_event_volume"),
                F.avg("processing_time_sec").alias("avg_processing_time"),
                F.sum("is_delayed").alias("total_delayed_events")
            ) \
            .withColumn("delay_rate", F.round(F.col("total_delayed_events") / F.col("total_event_volume"), 4)) \
            .filter(F.col("total_event_volume") > 100) \
            .orderBy(F.desc("delay_rate"))

        df_bottlenecks.persist(StorageLevel.MEMORY_AND_DISK)
        
        print("\n⚠️ TOP WAREHOUSE BOTTLENECK ZONES IDENTIFIED:")
        df_bottlenecks.show(10, truncate=False)
        
        df_bottlenecks.coalesce(1) \
            .write \
            .mode("overwrite") \
            .format("parquet") \
            .save(output_report_path)
            
        print(f"✅ Analysis saved successfully to Parquet format at: {output_report_path}")
        
        df_bottlenecks.unpersist()

    except Exception as e:
        print(f"Pipeline Execution Failed: {str(e)}")
        raise

    finally:
        spark.stop()
        print("Spark session closed.")

if __name__ == "__main__":
    # CRITICAL WINDOWS FIX: Prepend file:/// URI protocol and turn all backward slashes (\) into forward slashes (/)
    INPUT_PATH = "file:///C:/Users/girishkumar.bm/Downloads/Projects/pyspark_exercise_project/data/wms_events"
    OUTPUT_PATH = "file:///C:/Users/girishkumar.bm/Downloads/Projects/pyspark_exercise_project/data/output/bottleneck_report"

    run_wms_bottleneck_pipeline(data_input_path=INPUT_PATH, output_report_path=OUTPUT_PATH)
