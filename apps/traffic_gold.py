from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, when


SILVER_PATH = "/opt/spark/warehouse/traffic_silver"
DIM_ZONE_PATH = "/opt/spark/warehouse/dim_zone"
DIM_ROAD_PATH = "/opt/spark/warehouse/dim_road"
FACT_TRAFFIC_PATH = "/opt/spark/warehouse/fact_traffic"
GOLD_CHECKPOINT = "/opt/spark/warehouse/chk/traffic_gold"


spark = (
    SparkSession.builder
    .appName("TrafficGoldLayer")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "2")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


silver_stream = (
    spark.readStream
    .format("delta")
    .option("startingVersion", 0)
    .load(SILVER_PATH)
)


def process_gold_batch(batch_df, batch_id: int) -> None:
    batch_spark = batch_df.sparkSession

    if batch_df.isEmpty():
        print(f"[gold] batch {batch_id}: no new rows", flush=True)
        return

    print(f"[gold] batch {batch_id}: rebuilding star schema from silver snapshot", flush=True)

    silver_snapshot = batch_spark.read.format("delta").load(SILVER_PATH).coalesce(2).cache()

    dim_zone_df = (
        silver_snapshot.select("city_zone")
        .where(col("city_zone").isNotNull())
        .dropDuplicates()
        .withColumn(
            "zone_type",
            when(col("city_zone") == "CBD", "Commercial")
            .when(col("city_zone") == "TECHPARK", "IT HUB")
            .when(col("city_zone").isin("AIRPORT", "TRAINSTATION"), "TRANSIT HUB")
            .otherwise("Residential")
        )
        .withColumn(
            "traffic_risk",
            when(col("city_zone").isin("CBD", "AIRPORT"), "HIGH")
            .when(col("city_zone") == "TECHPARK", "MEDIUM")
            .otherwise("LOW")
        )
    )

    dim_road_df = (
        silver_snapshot.select("road_id")
        .where(col("road_id").isNotNull())
        .dropDuplicates()
        .withColumn(
            "road_type",
            when(col("road_id").isin("R100", "R200"), "Highway").otherwise("City Road")
        )
        .withColumn(
            "speed_limit",
            when(col("road_id").isin("R100", "R200"), lit(100)).otherwise(lit(60))
        )
    )

    fact_traffic_df = (
        silver_snapshot.select(
            "vehicle_id",
            "road_id",
            "city_zone",
            "speed_int",
            "congestion_level",
            "event_ts",
            "peak_flag",
            "speed_band",
            "hour",
            "weather",
        )
        .where(col("vehicle_id").isNotNull() & col("event_ts").isNotNull())
        .dropDuplicates(["vehicle_id", "event_ts"])
        .withColumn("date", to_date("event_ts"))
    )

    (
        dim_zone_df.coalesce(1).write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(DIM_ZONE_PATH)
    )

    (
        dim_road_df.coalesce(1).write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(DIM_ROAD_PATH)
    )

    (
        fact_traffic_df.coalesce(1).write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(FACT_TRAFFIC_PATH)
    )

    silver_snapshot.unpersist()

    print(f"[gold] batch {batch_id}: gold tables written", flush=True)


gold_query = (
    silver_stream.writeStream
    .foreachBatch(process_gold_batch)
    .outputMode("append")
    .trigger(availableNow=True)
    .option("checkpointLocation", GOLD_CHECKPOINT)
    .start()
)

gold_query.awaitTermination()
