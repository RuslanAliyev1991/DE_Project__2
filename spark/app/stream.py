from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, explode, from_json, count, avg, current_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType, BooleanType
import os
import sys

# spark://localhost:7077
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

DB_URL = "jdbc:postgresql://postgres_source:5432/source_db"
DB_PROPERTIES = {
    "user": "source_realTime",
    "password": "source_realTime",
    "driver": "org.postgresql.Driver"
}

def write_to_raw(batch_df, batch_id):    
    if batch_df.count() > 0:
        batch_df.write.jdbc(url=DB_URL, table="raw.kafka_raw_flights", mode="append", properties=DB_PROPERTIES)

def write_to_metrics(batch_df, batch_id):
    if batch_df.count() > 0:
        flat_df = batch_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("active_flights_count"),
            col("average_altitude")
        )
        flat_df.write.jdbc(url=DB_URL, table="silver.flight_metrics", mode="append", properties=DB_PROPERTIES)

def write_to_anomalies(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write.jdbc(url=DB_URL, table="silver.flight_anomalies", mode="append", properties=DB_PROPERTIES)

def write_to_coords(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write.jdbc(url=DB_URL, table="silver.flight_coordinates", mode="append", properties=DB_PROPERTIES)


spark = SparkSession.builder.appName("OpenSkyKafkaToPostgres").getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9092") \
    .option("subscribe", "mytopic") \
    .option("startingOffsets", "latest") \
    .load()

# RAW DATA:
raw_json_df = kafka_df.select(col("value").cast("string").alias("payload"))
query = raw_json_df.writeStream.foreachBatch(write_to_raw).start()
#======================================================================================

opensky_schema = StructType([
    StructField("time", LongType(), True),
    StructField("states", ArrayType(ArrayType(StringType())), True)
])

parsed_df = raw_json_df.select(from_json(col("payload"), opensky_schema).alias("data"))
exploded_df = parsed_df.select(explode(col("data.states")).alias("flight_array"))
silver_df = exploded_df.select(
    current_timestamp().alias("timestamp"),
    col("flight_array").getItem(0).alias("icao24"),
    col("flight_array").getItem(1).alias("callsign"),
    col("flight_array").getItem(2).alias("origin_country"),
    col("flight_array").getItem(5).cast(DoubleType()).alias("longitude"),
    col("flight_array").getItem(6).cast(DoubleType()).alias("latitude"),
    col("flight_array").getItem(7).cast(DoubleType()).alias("baro_altitude"),
    col("flight_array").getItem(9).cast(DoubleType()).alias("vertical_rate")
).filter(col("icao24").isNotNull())

#===============================================================================================
metrics_df = silver_df.withWatermark("timestamp", "10 seconds")\
    .groupBy(window(col("timestamp"), "1 minute")).agg(
        count("icao24").alias("active_flights_count"),
        avg("baro_altitude").alias("average_altitude")
    )
query_metrics = metrics_df.writeStream.foreachBatch(write_to_metrics).start()
#===============================================================================================

anomalies_df = silver_df.filter("vertical_rate > 15 OR vertical_rate < -15")\
    .select("timestamp", "icao24", "callsign", "vertical_rate")
query_anomalies = anomalies_df.writeStream.foreachBatch(write_to_anomalies).start()
#===============================================================================================

coords_df = silver_df.select("timestamp", "icao24", "callsign", "latitude", "longitude")
query_coords = coords_df.writeStream.foreachBatch(write_to_coords).start()
#===============================================================================================

# butun axinlarin daimi aciq qalmasi ucun gozleyirik
spark.streams.awaitAnyTermination()
spark.stop()
