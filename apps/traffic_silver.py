
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Spark Session Config 

spark = (
    SparkSession.builder
    .appName("TrafficSilverLayer")
    # clsuter master
    .master("spark://spark-master:7077")
    # delts lake
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)



spark.sparkContext.setLogLevel("WARN")


# Read Bronze Stream

bronze_df = (
    spark.readStream
    .format("delta")
    .option("startingVersion", 0)
    .load("/opt/spark/warehouse/traffic_bronze")
)

# Data Quality Flag

dq_df = bronze_df.withColumn(
    "dq_flag",
    when(col("vehicle_id").isNull(),"MISSING_VEHICLE")
    .when(col("event_time").isNull(), "MISSING_TIME")
    .when(col("raw_json").contains("CORRUPTED"),"CORRUPTED_JSON")
    .otherwise("OK")
)

# Safe Type Casting 

typed = dq_df.withColumn(
    "speed_int",
    col("speed").cast("int")
).withColumn(
    "event_ts",
    to_timestamp("event_time")
)

# Business Validation Rules

validated = typed.withColumn(
    "speed_valid",
    when((col("speed_int") >= 0) & (col("speed_int") <= 160), 1).otherwise(0)
).withColumn(
    "time_valid",
    when(col("event_ts") <= current_timestamp() + expr("INTERVAL 10 MINUTES"), 1).otherwise(0)
)

# Filter Good Records

clean_stream = validated.filter(
    (col("dq_flag") == "OK") &
    (col("speed_valid") == 1) &
    (col("time_valid") == 1)
)

# Handle Late Data

watermarked = clean_stream.withWatermark("event_ts", "15 minutes")

# Deduplication

deduped = watermarked.dropDuplicates(
    ["vehicle_id", "event_ts"]
)

# Feature Enginnering 

silver_final = deduped.withColumn(
    "hour", hour("event_ts")
).withColumn(
    "peak_flag",
    when((col("hour").between(17, 20)), 1).otherwise(0)
).withColumn(
    "speed_band",
    when(col("speed_int") < 30, "LOW")
    .when(col("speed_int") < 70, "MEDIUM")
    .otherwise("HIGH")
)

# Write Silver Table 

silver_query = (
    silver_final.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark/warehouse/chk/traffic_silver")
    .option("path","/opt/spark/warehouse/traffic_silver")
    .start()
)

spark.streams.awaitAnyTermination()
