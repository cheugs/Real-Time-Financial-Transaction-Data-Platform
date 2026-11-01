"""
The Spark logic of the pipeline, kept separate from the Kafka/Postgres code
so it can be unit-tested with small DataFrames.

  parse_events()      JSON text -> typed columns
  add_validation()    adds amount_eur and error_reason (NULL = valid)
  volume_per_window() event-time windowed aggregates with watermark + dedup
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

STATUSES = ["APPROVED", "DECLINED", "PENDING"]
MAX_AMOUNT_EUR = 1_000_000     # anything bigger is treated as a data error
MAX_FUTURE_MINUTES = 5         # small clock differences are OK, bigger ones are not
LATE_THRESHOLD_S = 60          # "late" = reached Kafka more than 60 s after event_time

# The schema contains the fields from ALL versions.
# v1 events simply have null for the v2 fields -> backward compatible.
# Unknown extra fields (from a future v3) are ignored -> forward compatible.
PAYMENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("schema_version", IntegerType()),
    StructField("account_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("country", StringType()),
    StructField("status", StringType()),
    StructField("event_time", StringType()),
    # added in schema version 2 (optional)
    StructField("payment_method", StringType()),
    StructField("device_id", StringType()),
])


def parse_events(df: DataFrame) -> DataFrame:
    """
    Input columns : raw_value (JSON string), kafka_time, kafka_partition, kafka_offset
    Output        : the same columns + all event fields, event_time as a timestamp.
    Malformed JSON gives a row where every event field is null.
    """
    return (
        df.withColumn("data", F.from_json("raw_value", PAYMENT_SCHEMA))
        .select("raw_value", "kafka_time", "kafka_partition", "kafka_offset", "data.*")
        .withColumnRenamed("event_time", "event_time_text")
        .withColumn("event_time", F.to_timestamp("event_time_text"))
        # events without a version number come from the oldest producers = v1
        .withColumn("schema_version", F.coalesce(F.col("schema_version"), F.lit(1)))
    )


def add_validation(df: DataFrame, fx_rates: dict) -> DataFrame:
    """
    Adds:
      amount_eur   - amount converted with the fx_rates table
      error_reason - why the record is invalid, or NULL if it is valid
    The FIRST rule that fails is reported.
    """
    # CASE WHEN currency = 'EUR' THEN 1.0 WHEN ... END  (NULL for unknown currencies)
    fx_rate = None
    for cur, rate in fx_rates.items():
        condition = F.col("currency") == cur
        fx_rate = F.when(condition, float(rate)) if fx_rate is None else fx_rate.when(condition, float(rate))
    df = (
        df.withColumn("fx_rate", fx_rate)
        .withColumn("amount_eur", F.round(F.col("amount") * F.col("fx_rate"), 2))
    )

    c = F.col
    error = (
        F.when(c("event_id").isNull() & c("account_id").isNull() & c("amount").isNull(), "malformed_json")
        .when(c("event_id").isNull(), "missing_event_id")
        .when(c("account_id").isNull(), "missing_account_id")
        .when(c("amount").isNull(), "missing_amount")
        .when(c("event_time_text").isNull(), "missing_event_time")
        .when(c("event_time").isNull(), "invalid_event_time")
        .when(c("event_time") > F.current_timestamp() + F.expr(f"INTERVAL {MAX_FUTURE_MINUTES} MINUTES"),
              "event_time_in_future")
        .when(c("amount") < 0.01, "non_positive_amount")   # 0.01 = smallest valid amount
        .when(c("currency").isNull() | c("fx_rate").isNull(), "unknown_currency")
        .when(c("amount_eur") > MAX_AMOUNT_EUR, "amount_too_large")
        .when(c("status").isNull() | ~c("status").isin(STATUSES), "invalid_status")
    )
    return df.withColumn("error_reason", error)


def volume_per_window(valid: DataFrame, watermark: str = "10 minutes", window: str = "1 minute") -> DataFrame:
    """
    Transaction volume per EVENT-TIME window.

    withWatermark  -> Spark keeps windows open for late events up to `watermark`
                      behind the newest event_time seen; older events are dropped.
    dropDuplicates -> the same event_id is only counted once. Including event_time
                      lets Spark clean up the dedup state after the watermark.
    """
    with_delay = valid.withColumn(
        # how late did the event reach Kafka compared to when it happened?
        "arrival_delay_s", F.col("kafka_time").cast("double") - F.col("event_time").cast("double")
    )
    return (
        with_delay
        .withWatermark("event_time", watermark)
        .dropDuplicates(["event_id", "event_time"])
        .groupBy(F.window("event_time", window))
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount_eur"), 2).alias("total_amount_eur"),
            F.sum(F.when(F.col("status") == "APPROVED", 1).otherwise(0)).alias("approved_count"),
            F.sum(F.when(F.col("status") == "DECLINED", 1).otherwise(0)).alias("declined_count"),
            F.round(F.avg("arrival_delay_s"), 2).alias("avg_arrival_delay_s"),
            F.round(F.max("arrival_delay_s"), 2).alias("max_arrival_delay_s"),
            F.sum(F.when(F.col("arrival_delay_s") > LATE_THRESHOLD_S, 1).otherwise(0)).alias("late_event_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "txn_count", "total_amount_eur", "approved_count", "declined_count",
            "avg_arrival_delay_s", "max_arrival_delay_s", "late_event_count",
        )
    )
