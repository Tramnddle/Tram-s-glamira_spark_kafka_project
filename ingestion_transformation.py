import os

import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

import util.config as conf
from util.logger import Log4j
from util.transform_utils import clean_url_udf, extract_domain_udf, normalize_lower, normalize_string
from browser.browser import parse_browser_udf


EVENT_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("time_stamp", LongType(), True),
    StructField("ip", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("resolution", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("api_version", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("local_time", StringType(), True),
    StructField("show_recommendation", BooleanType(), True),
    StructField("current_url", StringType(), True),
    StructField("referrer_url", StringType(), True),
    StructField("email", StringType(), True),
    StructField("collection", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("option", ArrayType(StructType([
        StructField("option_id", StringType(), True),
        StructField("option_label", StringType(), True),
    ])), True),
])


if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"base_dir: {base_dir}")

    conf_path_file = base_dir + "/spark.conf"

    conf = conf.Config(conf_path_file)

    spark_conf = conf.spark_conf
    kafka_conf = conf.kafka_conf
    kafka_conf.setdefault("startingOffsets", "latest")

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    log.info(f"spark_conf: {spark_conf.getAll()}")
    safe_kafka_conf = {
        key: ("***redacted***" if key == "kafka.sasl.jaas.config" else value)
        for key, value in kafka_conf.items()
    }
    log.info(f"kafka_conf: {safe_kafka_conf.items()}")

    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_conf) \
        .load()

    log.info(f"isStreaming: {df.isStreaming}")
    df.printSchema()

    parsed_df = df.select(
        f.from_json(col("value").cast(StringType()), EVENT_SCHEMA).alias("event")
    ).where(col("event").isNotNull()).select("event.*")

    parsed_df.printSchema()

    cleaned_df = parsed_df \
        .withColumn("id", normalize_string(col("id"))) \
        .withColumn("api_version", normalize_string(col("api_version"))) \
        .withColumn("collection", normalize_lower(col("collection"))) \
        .withColumn("current_url", clean_url_udf(normalize_string(col("current_url")))) \
        .withColumn("referrer_url", clean_url_udf(normalize_string(col("referrer_url")))) \
        .withColumn("device_id", normalize_string(col("device_id"))) \
        .withColumn("email", normalize_string(col("email"))) \
        .withColumn("ip", normalize_string(col("ip"))) \
        .withColumn("local_time", normalize_string(col("local_time"))) \
        .withColumn("local_ts", f.to_timestamp(col("local_time"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn(
            "event_ts",
            f.when(
                col("time_stamp").isNotNull(),
                f.when(col("time_stamp") > 1000000000000,
                       f.to_timestamp((col("time_stamp") / 1000).cast("double")))
                 .otherwise(f.to_timestamp(col("time_stamp").cast("double")))
            )
        ) \
        .withColumn("product_id", normalize_string(col("product_id"))) \
        .withColumn("store_id", normalize_string(col("store_id"))) \
        .withColumn("show_recommendation", col("show_recommendation")) \
        .withColumn("user_agent", normalize_string(col("user_agent"))) \
        .withColumn("current_domain", extract_domain_udf(col("current_url"))) \
        .withColumn("referrer_domain", extract_domain_udf(col("referrer_url"))) \
        .withColumn("browser", f.when(col("user_agent").isNotNull(), parse_browser_udf(col("user_agent")))) \
        .withColumn("event_date", f.to_date(col("event_ts"))) \
        .withColumn("event_hour", f.hour(col("event_ts"))) \
        .where(
            col("id").isNotNull() &
            col("current_url").isNotNull() &
            col("local_ts").isNotNull() &
            col("event_ts").isNotNull()
        )

    cleaned_df = cleaned_df \
        .withWatermark("event_ts", "1 day") \
        .dropDuplicates(["id"])

    cleaned_df.printSchema()

    # Write to HDFS
    hdfs_query = cleaned_df \
        .writeStream \
        .format("parquet") \
        .mode("append") \
        .option("path", "hdfs:///user/spark/data/glamira/cleaned_events") \
        .option("checkpointLocation", "hdfs:///user/spark/data/glamira/checkpoint") \
        .trigger(processingTime="30 seconds") \
        .start()

    # Also display to console for monitoring
    console_query = cleaned_df \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .option("numRows", 20) \
        .trigger(processingTime="30 seconds") \
        .start()

    hdfs_query.awaitTermination()