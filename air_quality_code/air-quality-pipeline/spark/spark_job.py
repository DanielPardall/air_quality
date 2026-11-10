from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "air_quality"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/air_quality_db"
POSTGRES_USER = "iacd_user"
POSTGRES_PASS = "iacd_pass"

RAW_TABLE     = "air_raw"
DAILY_TABLE   = "air_daily"
MONTHLY_TABLE = "air_monthly"
ALERTS_TABLE  = "air_alerts"

THRESHOLD_CO  = 600.0  
THRESHOLD_NO2 = 100.0
THRESHOLD_O3  = 90.0

spark = (
    SparkSession.builder
    .appName("AirQualityKafkaToPostgres")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

event_schema = T.StructType([
    T.StructField("sensor_id",   T.IntegerType(), nullable=False),
    T.StructField("location_id", T.IntegerType(), nullable=True),
    T.StructField("parameter",   T.StringType(), nullable=False),
    T.StructField("unit",        T.StringType(), nullable=True),
    T.StructField("value",       T.DoubleType(), nullable=True),
    T.StructField("datetime_utc",T.StringType(), nullable=False),
])

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

df_parsed = (
    df_kafka
    .select(
        F.col("value").cast("string").alias("value_str")
    )
    .withColumn("data", F.from_json("value_str", event_schema))
    .select("data.*")
)

df_parsed = df_parsed.withColumn(
    "datetime_ts", F.to_timestamp("datetime_utc")
)

df_parsed = df_parsed.filter(
    (F.col("value").isNotNull()) & (F.col("value") >= 0)
)


def write_to_postgres(batch_df, batch_id: int):

    if batch_df.rdd.isEmpty():
        return

    raw_df = (
        batch_df
        .select(
            "sensor_id",
            "location_id",
            "parameter",
            "unit",
            "value",
            F.col("datetime_ts").alias("datetime_utc"),
            F.current_timestamp().alias("ingested_at"),
        )
    )

    (
        raw_df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", RAW_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )



    daily_df = (
        batch_df
        .withColumn("date", F.to_date("datetime_ts"))
        .groupBy(
            "date",
            "parameter",
            "sensor_id",
            "location_id",
        )
        .agg(
            F.avg("value").alias("avg_value"),
            F.max("value").alias("max_value"),
            F.min("value").alias("min_value"),
            F.count("*").alias("num_hours"),
        )
        .dropDuplicates(["date", "parameter", "sensor_id", "location_id"])
    )

    (
        daily_df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", DAILY_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    monthly_df = (
        batch_df
        .withColumn("month_date", F.to_date(F.date_trunc("month", "datetime_ts")))
        .groupBy(
            "month_date",
            "parameter",
            "sensor_id",
            "location_id",
        )
        .agg(
            F.avg("value").alias("avg_value"),
            F.max("value").alias("max_value"),
            F.min("value").alias("min_value"),
            F.count("*").alias("num_hours"),
        )
        .dropDuplicates(["month_date", "parameter", "sensor_id", "location_id"])
    )

    (
        monthly_df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", MONTHLY_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    daily_with_threshold = (
        daily_df
        .withColumn(
            "threshold",
            F.when(F.col("parameter") == "co",  F.lit(THRESHOLD_CO))
             .when(F.col("parameter") == "no2", F.lit(THRESHOLD_NO2))
             .when(F.col("parameter") == "o3",  F.lit(THRESHOLD_O3))
             .otherwise(F.lit(None).cast("double"))
        )
    )

    alerts_df = (
        daily_with_threshold
        .filter(
            F.col("threshold").isNotNull()
            & (F.col("max_value") > F.col("threshold"))
        )
        .withColumn("num_exceedances", F.lit(1))
        .select(
            "date",
            "parameter",
            "threshold",
            "num_exceedances",
            "max_value",
            "sensor_id",
            "location_id",
        )
        .dropDuplicates(["date", "parameter", "sensor_id", "location_id", "threshold"])
    )

    if not alerts_df.rdd.isEmpty():
        (
            alerts_df.write
            .format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", ALERTS_TABLE)
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASS)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )

query = (
    df_parsed
    .writeStream
    .outputMode("append")
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "checkpoints/air_quality_postgres")
    .start()
)

query.awaitTermination()
