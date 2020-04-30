import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

def run_spark_job(spark):
    
    spark.sparkContext.setLogLevel("WARN")
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "crime-reports.v1") \
        .option("startingOffsets", "earliest") \
        .option("maxRatePerPartition", 2) \
        .option("maxOffsetPerTrigger", 100) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    schema = StructType([
        StructField("crime_id", StringType(), True),
        StructField("original_crime_type_name", StringType(), True),
        StructField("report_date", TimestampType(), True),
        StructField("call_date", TimestampType(), True),
        StructField("call_time", StringType(), True),
        StructField("call_date_time", TimestampType(), True),
        StructField("disposition", StringType(), True),
        StructField("address", StringType(), True),
        StructField("agency_id", StringType(), True),
        StructField("address_type", StringType(), True),
        StructField("common_location", StringType(), True)
    ])
    
    # Extract the correct column from the kafka input resources
    # Take only value and convert it to String
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # Select original_crime_type_name and disposition
    distinct_table = service_table \
                        .select('original_crime_type_name', 'disposition', 'call_date_time') \
                        .distinct()

    # Count the number of original crime type
    agg_df = distinct_table \
                .select('original_crime_type_name', 'call_date_time') \
                .withWatermark("call_date_time", '10 minutes') \
                .groupBy("original_crime_type_name", psf.window('call_date_time', "10 minutes", "5 minutes")) \
                .count()
    
    # Schema for different crime types
    agg_df.printSchema()
    
    # Q1. Submit a screen shot of a batch ingestion of the aggregation
    query = agg_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

    query.awaitTermination()

#     schema_radio_code = StructType([
#         StructField("disposition_code", StringType(), True),
#         StructField("description", StringType(), True)
#     ])

#     radio_code_json_filepath = "radio_code.json"
#     radio_code_df = spark.read.option("multiline", "true").json(radio_code_json_filepath, schema_radio_code) 

#     # clean up your data so that the column names match on radio_code_df and agg_df
#     # we will want to join on the disposition code
#     radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

#     join_query = distinct_table \
#                     .join(radio_code_df, "disposition", "left") \
#                     .select("call_date_time", "original_crime_type_name", "description")

#     # Write stream to console
#     join_query \
#             .writeStream \
#             .trigger(processingTime="10 seconds") \
#             .outputMode("append") \
#             .format("console") \
#             .start()
    
#     join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", "3000") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
