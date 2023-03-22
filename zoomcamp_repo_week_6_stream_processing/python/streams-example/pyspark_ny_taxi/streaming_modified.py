from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import count, desc
import pyspark.sql.types as T

# from settings import TOPIC_WINDOWED_VENDOR_ID_COUNT
CONSUME_TOPIC_RIDES_CSV_FHV = "fhv_csv"
CONSUME_TOPIC_RIDES_CSV_GREEN = "green_csv"

RIDE_SCHEMA_FHV = T.StructType(
    [
        T.StructField("dispatching_base_num", T.StringType()),
        T.StructField("pickup_datetime", T.TimestampType()),
        T.StructField("dropOff_datetime", T.TimestampType()),
        T.StructField("PUlocationID", T.IntegerType()),
        T.StructField("DOlocationID", T.IntegerType()),
        T.StructField("SR_Flag", T.IntegerType()),
        T.StructField("Affiliated_base_number", T.StringType()),
    ]
)

RIDE_SCHEMA_GREEN = T.StructType(
    [
        T.StructField("VendorID", T.IntegerType()),
        T.StructField("pickup_datetime", T.TimestampType()),
        T.StructField("dropOff_datetime", T.TimestampType()),
        T.StructField("store_and_fwd_flag", T.StringType()),
        T.StructField("RatecodeID", T.IntegerType()),
        T.StructField("PULocationID", T.IntegerType()),
        T.StructField("DOLocationID", T.StringType()),
        T.StructField("passenger_count", T.IntegerType()),
        T.StructField("trip_distance", T.FloatType()),
        T.StructField("fare_amount", T.FloatType()),
        T.StructField("extra", T.FloatType()),
        T.StructField("mta_tax", T.FloatType()),
        T.StructField("tip_amount", T.FloatType()),
        T.StructField("tolls_amount", T.FloatType()),
        T.StructField("ehail_fee", T.FloatType()),
        T.StructField("improvement_surcharge", T.FloatType()),
        T.StructField("total_amount", T.FloatType()),
        T.StructField("payment_type", T.IntegerType()),
        T.StructField("trip_type", T.IntegerType()),
        T.StructField("congestion_surcharge", T.FloatType()),
    ]
)


def read_from_kafka(consume_topic: str):
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    df_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092")
        .option("subscribe", consume_topic)
        .option("startingOffsets", "earliest")
        .option("checkpointLocation", "checkpoint")
        .load()
    )
    return df_stream


def parse_ride_from_kafka_message(df, schema):
    """take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema"""
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # split attributes to nested array in one Column
    col = F.split(df["value"], ", ")

    # expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])


def sink_console(df, output_mode: str = "complete", processing_time: str = "5 seconds"):
    write_query = (
        df.writeStream.outputMode(output_mode)
        .trigger(processingTime=processing_time)
        .format("console")
        .option("truncate", False)
        .start()
    )
    return write_query  # pyspark.sql.streaming.StreamingQuery


def op_groupby(df, column_names, topic):

    df_aggregation = (
        df.groupBy(column_names)
        .agg(count("PUlocationID").alias(f"ride_count_{topic}"))
        .sort(desc(f"ride_count_{topic}"))
    )
    return df_aggregation


if __name__ == "__main__":
    spark = SparkSession.builder.appName("streaming-PULocID").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # read_straming datae
    df_consume_stream_fhv = read_from_kafka(consume_topic=CONSUME_TOPIC_RIDES_CSV_FHV)

    # parse streaming data
    df_rides_fhv = parse_ride_from_kafka_message(df_consume_stream_fhv, RIDE_SCHEMA_FHV)
    # print(df_rides.printSchema())
    # sink_console(df_rides, output_mode='append')

    df_trip_count_by_pulocation_id_fhv = op_groupby(
        df_rides_fhv, ["PUlocationID"], topic="fhv"
    )

    # write the output out to the console
    sink_console(df_trip_count_by_pulocation_id_fhv)

    # Repeat for GREEN dataset
    df_consume_stream_green = read_from_kafka(
        consume_topic=CONSUME_TOPIC_RIDES_CSV_GREEN
    )
    df_rides_green = parse_ride_from_kafka_message(
        df_consume_stream_green, RIDE_SCHEMA_GREEN
    )
    df_trip_count_by_pulocation_id_green = op_groupby(
        df_rides_green, ["PULocationID"], topic="green"
    )
    sink_console(df_trip_count_by_pulocation_id_green)

    spark.streams.awaitAnyTermination()
