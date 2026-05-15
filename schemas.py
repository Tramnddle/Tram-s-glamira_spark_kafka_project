from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Spark schema for reading cleaned events data
OPTION_SCHEMA = ArrayType(
    StructType(
        [
            StructField("option_id", StringType(), True),
            StructField("option_label", StringType(), True),
        ]
    )
)

SCHEMA = StructType(
    [
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
        StructField("option", OPTION_SCHEMA, True),
        StructField("local_ts", TimestampType(), True),
        StructField("event_ts", TimestampType(), True),
        StructField("current_domain", StringType(), True),
        StructField("referrer_domain", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("event_date", DateType(), True),
        StructField("event_hour", IntegerType(), True),
    ]
)

# PostgreSQL table column definitions
DIM_DATE_COLUMNS = [
    "date_key",
    "date",
    "year",
    "quarter",
    "month",
    "month_name",
    "week_of_year",
    "day",
    "day_of_week",
    "day_name",
    "is_weekend",
]

DIM_PRODUCT_COLUMNS = [
    "product_key",
    "product_id",
    "option_id",
    "option_label",
    "valid_from",
    "valid_to",
    "is_current",
]

DIM_LOCATION_COLUMNS = [
    "location_key",
    "ip",
    "country",
    "country_code",
    "region",
    "city",
    "postal_code",
    "latitude",
    "longitude",
    "timezone",
    "isp",
]

DIM_CUSTOMER_COLUMNS = ["customer_key", "email"]

FACT_LOG_EVENT_COLUMNS = [
    "date_key",
    "product_key",
    "event_key",
    "event_id",
    "event_timestamp",
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
    "event_count",
    "ingestion_timestamp",
    "customer_key",
]

# Table configuration for PostgreSQL operations
TABLE_CONFIG = {
    "dim_date": {
        "key": "date_key",
        "columns": DIM_DATE_COLUMNS,
        "conflict_key": "date_key",
    },
    "dim_product": {
        "key": "product_key",
        "columns": DIM_PRODUCT_COLUMNS,
        "conflict_key": "product_key",
    },
    "dim_location": {
        "key": "location_key",
        "columns": DIM_LOCATION_COLUMNS,
        "conflict_key": "location_key",
    },
    "dim_customer": {
        "key": "customer_key",
        "columns": DIM_CUSTOMER_COLUMNS,
        "conflict_key": "customer_key",
        "update_columns": ["email"],
    },
    "fact_log_event": {
        "key": "event_key",
        "columns": FACT_LOG_EVENT_COLUMNS,
        "conflict_key": "event_key",
    },
}
