from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Spark Session Config 

spark = (
    SparkSession.builder
    .appName("TrafficStreamingLakeHouse")
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

# Kafka Raw Stream

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")     # internal listener
    .option("subscribe", "traffic-topic")
    .option("startingOffsets", "latest")
    .load()
)

# Convert Binary to String

json_stream = raw_stream.selectExpr(
    "CAST(value AS STRING) as raw_json",
    "timestamp as kafka_timestamp"
)

# Flexible Schema

traffic_schema = StructType([
    StructField("vehicle_id", StringType()),
    StructField("road_id", StringType()),
    StructField("city_zone", StringType()),
    StructField("speed", StringType()),
    StructField("congestion_level", IntegerType()),
    StructField("weather", StringType()),
    StructField("event_time", StringType()),    
])

parsed = json_stream.withColumn(
    "data",
    from_json(col("raw_json"),traffic_schema)
)

flattened = parsed.select(
    "raw_json",
    "kafka_timestamp",
    "data.*"
)

# Bronze Delta Write

bronze_query = (
    flattened.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark/warehouse/chk/traffic_bronze")
    .option("path","/opt/spark/warehouse/traffic_bronze")
    .start()
)

spark.streams.awaitAnyTermination()