import os
import re
from typing import Dict, List, Tuple

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

import util.config as conf
from util.location_utils import extract_country_code_udf, extract_country_udf
from util.logger import Log4j
from schemas import SCHEMA, TABLE_CONFIG, DIM_DATE_COLUMNS, DIM_PRODUCT_COLUMNS, DIM_LOCATION_COLUMNS, DIM_CUSTOMER_COLUMNS, FACT_LOG_EVENT_COLUMNS


def with_required_spark_package(existing_packages: str, required_package: str) -> str:
    packages = [package.strip() for package in existing_packages.split(",") if package.strip()]
    if required_package not in packages:
        packages.append(required_package)
    return ",".join(packages)


def get_spark_session() -> Tuple[SparkSession, Log4j]:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    conf_path_file = os.path.join(base_dir, "spark.conf")

    config = conf.Config(conf_path_file)
    spark_conf = config.spark_conf
    spark_conf.set("spark.app.name", "LoadCleanedEventsToPostgresStream")
    spark_conf.set(
        "spark.jars.packages",
        with_required_spark_package(
            spark_conf.get("spark.jars.packages", ""),
            os.environ.get("POSTGRES_JDBC_PACKAGE", "org.postgresql:postgresql:42.7.3"),
        ),
    )

    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark, Log4j(spark)


def get_runtime_config() -> Dict[str, str]:
    hdfs_base_uri = os.environ.get("HDFS_BASE_URI", "hdfs://namenode")
    cleaned_events_path = os.environ.get(
        "CLEANED_EVENTS_PATH",
        f"{hdfs_base_uri}/user/spark/data/glamira/cleaned_events",
    )

    jdbc_url = os.environ.get(
        "POSTGRES_JDBC_URL",
        f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'postgres')}:"
        f"{os.environ.get('POSTGRES_PORT', '5432')}/"
        f"{os.environ.get('POSTGRES_DB', 'glamira_analytics')}",
    )

    return {
        "cleaned_events_path": cleaned_events_path,
        "checkpoint_path": os.environ.get(
            "POSTGRES_STREAM_CHECKPOINT_PATH",
            f"{hdfs_base_uri}/user/spark/data/glamira/postgres_loader_checkpoint",
        ),
        "jdbc_url": jdbc_url,
        "postgres_schema": os.environ.get("POSTGRES_SCHEMA", "public"),
        "postgres_user": os.environ.get("POSTGRES_USER", "postgres"),
        "postgres_password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
        "jdbc_driver": os.environ.get("POSTGRES_JDBC_DRIVER", "org.postgresql.Driver"),
        "write_partitions": os.environ.get("POSTGRES_WRITE_PARTITIONS", "4"),
        "trigger_interval": os.environ.get("POSTGRES_TRIGGER_INTERVAL", "30 seconds"),
        "max_files_per_trigger": os.environ.get("POSTGRES_MAX_FILES_PER_TRIGGER", "10"),
        "query_name": os.environ.get("POSTGRES_QUERY_NAME", "glamira_postgres_loader"),
        "batch_log_table": os.environ.get("POSTGRES_BATCH_LOG_TABLE", "stream_batch_log"),
    }


def jdbc_options(runtime_conf: Dict[str, str]) -> Dict[str, str]:
    return {
        "url": runtime_conf["jdbc_url"],
        "user": runtime_conf["postgres_user"],
        "password": runtime_conf["postgres_password"],
        "driver": runtime_conf["jdbc_driver"],
    }


def is_empty(df: DataFrame) -> bool:
    return len(df.take(1)) == 0


def ensure_batch_log_table(spark: SparkSession, runtime_conf: Dict[str, str]) -> None:
    jvm = spark._jvm
    jvm.java.lang.Class.forName(runtime_conf["jdbc_driver"])
    connection = jvm.java.sql.DriverManager.getConnection(
        runtime_conf["jdbc_url"],
        runtime_conf["postgres_user"],
        runtime_conf["postgres_password"],
    )
    try:
        statement = connection.createStatement()
        try:
            batch_log_table = f"{runtime_conf['postgres_schema']}.{runtime_conf['batch_log_table']}"
            statement.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {batch_log_table} (
                    query_name TEXT NOT NULL,
                    batch_id BIGINT NOT NULL,
                    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (query_name, batch_id)
                )
                """.strip(),
            )
            statement.execute(
                f"""
                ALTER TABLE IF EXISTS {runtime_conf['postgres_schema']}.dim_customer
                ADD COLUMN IF NOT EXISTS email TEXT
                """.strip(),
            )
            statement.execute(
                f"""
                ALTER TABLE IF EXISTS {runtime_conf['postgres_schema']}.fact_log_event
                ADD COLUMN IF NOT EXISTS browser TEXT
                """.strip(),
            )
        finally:
            statement.close()
    finally:
        connection.close()


def is_batch_already_processed(
    spark: SparkSession,
    runtime_conf: Dict[str, str],
    batch_id: int,
) -> bool:
    jvm = spark._jvm
    jvm.java.lang.Class.forName(runtime_conf["jdbc_driver"])
    connection = jvm.java.sql.DriverManager.getConnection(
        runtime_conf["jdbc_url"],
        runtime_conf["postgres_user"],
        runtime_conf["postgres_password"],
    )
    try:
        batch_log_table = f"{runtime_conf['postgres_schema']}.{runtime_conf['batch_log_table']}"
        query = (
            f"SELECT 1 FROM {batch_log_table} "
            f"WHERE query_name = ? AND batch_id = ?"
        )
        prepared_statement = connection.prepareStatement(query)
        try:
            prepared_statement.setString(1, runtime_conf["query_name"])
            prepared_statement.setLong(2, int(batch_id))
            result_set = prepared_statement.executeQuery()
            try:
                return bool(result_set.next())
            finally:
                result_set.close()
        finally:
            prepared_statement.close()
    finally:
        connection.close()


def mark_batch_processed(connection, runtime_conf: Dict[str, str], batch_id: int) -> None:
    batch_log_table = f"{runtime_conf['postgres_schema']}.{runtime_conf['batch_log_table']}"
    query = (
        f"INSERT INTO {batch_log_table} "
        "(query_name, batch_id) VALUES (?, ?) "
        "ON CONFLICT (query_name, batch_id) DO NOTHING"
    )
    prepared_statement = connection.prepareStatement(query)
    try:
        prepared_statement.setString(1, runtime_conf["query_name"])
        prepared_statement.setLong(2, int(batch_id))
        prepared_statement.executeUpdate()
    finally:
        prepared_statement.close()


def staging_table_name(table_name: str, batch_id: int) -> str:
    return f"stg_{table_name}_{int(batch_id)}"


def all_staging_table_names(batch_id: int) -> List[str]:
    return [staging_table_name(table_name, batch_id) for table_name in TABLE_CONFIG]


def drop_staging_tables(
    spark: SparkSession,
    runtime_conf: Dict[str, str],
    table_names: List[str],
) -> None:
    if not table_names:
        return

    jvm = spark._jvm
    jvm.java.lang.Class.forName(runtime_conf["jdbc_driver"])
    connection = jvm.java.sql.DriverManager.getConnection(
        runtime_conf["jdbc_url"],
        runtime_conf["postgres_user"],
        runtime_conf["postgres_password"],
    )
    try:
        statement = connection.createStatement()
        try:
            for table_name in table_names:
                statement.execute(
                    f"DROP TABLE IF EXISTS {runtime_conf['postgres_schema']}.{table_name}"
                )
        finally:
            statement.close()
    finally:
        connection.close()


def write_staging_table(
    runtime_conf: Dict[str, str],
    df: DataFrame,
    staging_table: str,
) -> None:
    if is_empty(df):
        return

    write_partitions = max(int(runtime_conf["write_partitions"]), 1)
    dbtable = f"{runtime_conf['postgres_schema']}.{staging_table}"
    (
        df.repartition(write_partitions)
        .write.format("jdbc")
        .options(**jdbc_options(runtime_conf))
        .option("dbtable", dbtable)
        .mode("overwrite")
        .save()
    )


def merge_staging_table(connection, runtime_conf: Dict[str, str], target_table: str, staging_table: str) -> None:
    columns = TABLE_CONFIG[target_table]["columns"]
    conflict_key = TABLE_CONFIG[target_table]["conflict_key"]
    update_columns = TABLE_CONFIG[target_table].get("update_columns", [])
    column_csv = ", ".join(columns)
    target_table_name = f"{runtime_conf['postgres_schema']}.{target_table}"
    staging_table_name = f"{runtime_conf['postgres_schema']}.{staging_table}"
    on_conflict_clause = f"ON CONFLICT ({conflict_key}) DO NOTHING"

    if update_columns:
        update_clause = ", ".join(
            f"{column_name} = COALESCE(EXCLUDED.{column_name}, {target_table_name}.{column_name})"
            for column_name in update_columns
        )
        on_conflict_clause = f"ON CONFLICT ({conflict_key}) DO UPDATE SET {update_clause}"

    statement = connection.createStatement()
    try:
        statement.execute(
            (
                f"INSERT INTO {target_table_name} ({column_csv}) "
                f"SELECT {column_csv} FROM {staging_table_name} "
                f"{on_conflict_clause}"
            )
        )
    finally:
        statement.close()


def prepare_base_dataframe(cleaned_df: DataFrame) -> DataFrame:
    first_option = col("option").getItem(0)

    return (
        cleaned_df.withColumn("option_id", first_option.getField("option_id"))
        .withColumn("option_label", first_option.getField("option_label"))
        .withColumn("date_key", f.date_format(col("event_date"), "yyyyMMdd"))
        .withColumn(
            "product_key",
            f.when(
                col("product_id").isNotNull(),
                f.sha2(
                    f.concat_ws(
                        "||",
                        col("product_id"),
                        f.coalesce(col("option_id"), f.lit("")),
                        f.coalesce(col("option_label"), f.lit("")),
                    ),
                    256,
                ),
            ),
        )
        .withColumn(
            "customer_key",
            f.when(col("email").isNotNull(), f.sha2(f.lower(col("email")), 256)),
        )
        .withColumn("country_code", extract_country_code_udf(col("current_domain")))
        .withColumn("country", extract_country_udf(col("current_domain")))
        .withColumn(
            "location_key",
            f.when(
                col("ip").isNotNull() | col("country_code").isNotNull(),
                f.sha2(
                    f.concat_ws(
                        "||",
                        f.coalesce(col("ip"), f.lit("")),
                        f.coalesce(col("country_code"), f.lit("")),
                    ),
                    256,
                ),
            ),
        )
        .withColumn("event_key", col("id"))
        .dropDuplicates(["event_key"])
    )


def build_dim_date(base_df: DataFrame) -> DataFrame:
    return (
        base_df.select("date_key", "event_date")
        .where(col("date_key").isNotNull() & col("event_date").isNotNull())
        .dropDuplicates(["date_key"])
        .select(
            "date_key",
            col("event_date").alias("date"),
            f.year(col("event_date")).alias("year"),
            f.quarter(col("event_date")).alias("quarter"),
            f.month(col("event_date")).alias("month"),
            f.date_format(col("event_date"), "MMMM").alias("month_name"),
            f.weekofyear(col("event_date")).alias("week_of_year"),
            f.dayofmonth(col("event_date")).alias("day"),
            f.dayofweek(col("event_date")).alias("day_of_week"),
            f.date_format(col("event_date"), "EEEE").alias("day_name"),
            f.dayofweek(col("event_date")).isin([1, 7]).alias("is_weekend"),
        )
    )


def build_dim_product(base_df: DataFrame) -> DataFrame:
    return (
        base_df.select("product_key", "product_id", "option_id", "option_label", "event_date")
        .where(col("product_key").isNotNull())
        .groupBy("product_key", "product_id", "option_id", "option_label")
        .agg(f.min("event_date").alias("valid_from"))
        .select(
            "product_key",
            "product_id",
            "option_id",
            "option_label",
            "valid_from",
            f.lit(None).cast("date").alias("valid_to"),
            f.lit(True).alias("is_current"),
        )
    )


def build_dim_location(base_df: DataFrame) -> DataFrame:
    return (
        base_df.select("location_key", "ip", "country", "country_code")
        .where(col("location_key").isNotNull())
        .dropDuplicates(["location_key"])
        .select(
            "location_key",
            "ip",
            "country",
            "country_code",
            f.lit(None).cast("string").alias("region"),
            f.lit(None).cast("string").alias("city"),
            f.lit(None).cast("string").alias("postal_code"),
            f.lit(None).cast("double").alias("latitude"),
            f.lit(None).cast("double").alias("longitude"),
            f.lit(None).cast("string").alias("timezone"),
            f.lit(None).cast("string").alias("isp"),
        )
    )


def build_dim_customer(base_df: DataFrame) -> DataFrame:
    return (
        base_df.select(
            "customer_key",
            f.lower(col("email")).alias("email"),
        )
        .where(col("customer_key").isNotNull())
        .dropDuplicates(["customer_key"])
    )


def build_fact_log_event(base_df: DataFrame) -> DataFrame:
    return (
        base_df.select(
            "date_key",
            "product_key",
            "event_key",
            col("id").alias("event_id"),
            col("event_ts").alias("event_timestamp"),
            "ip",
            "device_id",
            "user_agent",
            "browser",
            "resolution",
            "api_version",
            "location_key",
            "store_id",
            "local_time",
            "collection",
            "current_url",
            "referrer_url",
            "show_recommendation",
            f.lit(1).alias("event_count"),
            f.current_timestamp().alias("ingestion_timestamp"),
            "customer_key",
        )
        .where(col("event_key").isNotNull())
        .dropDuplicates(["event_key"])
    )


def build_table_dataframes(base_df: DataFrame) -> Dict[str, DataFrame]:
    return {
        "dim_date": build_dim_date(base_df),
        "dim_product": build_dim_product(base_df),
        "dim_location": build_dim_location(base_df),
        "dim_customer": build_dim_customer(base_df),
        "fact_log_event": build_fact_log_event(base_df),
    }


def summarize_table_counts(table_dfs: Dict[str, DataFrame], table_names: List[str]) -> str:
    return ", ".join(f"{table_name}={table_dfs[table_name].count()}" for table_name in table_names)


def process_microbatch(batch_df: DataFrame, batch_id: int, runtime_conf: Dict[str, str]) -> None:
    spark = batch_df.sparkSession
    log = Log4j(spark)

    if is_empty(batch_df):
        log.info(f"batch_id={batch_id}: empty micro-batch, skipping")
        return

    if is_batch_already_processed(spark, runtime_conf, batch_id):
        drop_staging_tables(spark, runtime_conf, all_staging_table_names(batch_id))
        log.info(f"batch_id={batch_id}: already processed, skipping")
        return

    base_df = prepare_base_dataframe(batch_df).cache()
    input_row_count = base_df.count()
    table_dfs = build_table_dataframes(base_df)

    non_empty_table_names = [
        table_name for table_name, df in table_dfs.items() if not is_empty(df)
    ]
    if not non_empty_table_names:
        log.info(f"batch_id={batch_id}: all target DataFrames are empty")
        base_df.unpersist()
        return

    log.info(
        f"batch_id={batch_id}: input_rows={input_row_count}, "
        f"{summarize_table_counts(table_dfs, non_empty_table_names)}"
    )

    staged_tables: List[str] = []
    try:
        for table_name in non_empty_table_names:
            staging_table = staging_table_name(table_name, batch_id)
            write_staging_table(runtime_conf, table_dfs[table_name], staging_table)
            staged_tables.append(staging_table)

        jvm = spark._jvm
        jvm.java.lang.Class.forName(runtime_conf["jdbc_driver"])
        connection = jvm.java.sql.DriverManager.getConnection(
            runtime_conf["jdbc_url"],
            runtime_conf["postgres_user"],
            runtime_conf["postgres_password"],
        )
        try:
            connection.setAutoCommit(False)

            for table_name in ["dim_date", "dim_product", "dim_location", "dim_customer", "fact_log_event"]:
                if table_name not in non_empty_table_names:
                    continue
                merge_staging_table(
                    connection,
                    runtime_conf,
                    table_name,
                    staging_table_name(table_name, batch_id),
                )

            mark_batch_processed(connection, runtime_conf, batch_id)
            connection.commit()
            log.info(f"batch_id={batch_id}: committed successfully")
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
    finally:
        base_df.unpersist()
        drop_staging_tables(spark, runtime_conf, staged_tables)


if __name__ == "__main__":
    spark, log = get_spark_session()
    runtime_conf = get_runtime_config()

    log.info(f"cleaned_events_path: {runtime_conf['cleaned_events_path']}")
    log.info(f"checkpoint_path: {runtime_conf['checkpoint_path']}")
    log.info(f"postgres_schema: {runtime_conf['postgres_schema']}")
    log.info(f"trigger_interval: {runtime_conf['trigger_interval']}")
    log.info(f"query_name: {runtime_conf['query_name']}")

    ensure_batch_log_table(spark, runtime_conf)

    stream_df = (
        spark.readStream.format("parquet")
        .schema(SCHEMA)
        .option("maxFilesPerTrigger", runtime_conf["max_files_per_trigger"])
        .load(runtime_conf["cleaned_events_path"])
    )

    query = (
        stream_df.writeStream.foreachBatch(
            lambda batch_df, batch_id: process_microbatch(batch_df, batch_id, runtime_conf)
        )
        .outputMode("append")
        .option("checkpointLocation", runtime_conf["checkpoint_path"])
        .queryName(runtime_conf["query_name"])
        .trigger(processingTime=runtime_conf["trigger_interval"])
        .start()
    )

    query.awaitTermination()
