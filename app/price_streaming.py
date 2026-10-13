
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

spark = (
    SparkSession.builder
    .appName("EcommercePriceStreaming")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("product_id", StringType()),
    StructField("platform", StringType()),
    StructField("price", LongType()),
    StructField("recorded_at", StringType())
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "product-prices")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw.select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
)

processed = (
    parsed
    .filter(col("product_id").isNotNull())
    .filter(col("platform").isNotNull())
    .filter(col("price") > 0)
    .withColumn(
        "recorded_at",
        to_timestamp(
            "recorded_at",
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .filter(col("recorded_at").isNotNull())
    .withColumn("processed_at", current_timestamp())
)

jdbc_url = "jdbc:postgresql://postgres:5432/ecommerce"

properties = {
    "user": "demo",
    "password": "demo123",
    "driver": "org.postgresql.Driver"
}

def save_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    data = batch_df.cache()

    try:
        total = data.count()

        data.write \
            .mode("append") \
            .parquet("/opt/demo/lake/prices")

        data.write.jdbc(
            url=jdbc_url,
            table="price_history",
            mode="append",
            properties=properties
        )

        print(
            f"BATCH {batch_id}: "
            f"Da luu {total} ban ghi gia",
            flush=True
        )

    finally:
        data.unpersist()

query = (
    processed.writeStream
    .foreachBatch(save_batch)
    .option(
        "checkpointLocation",
        "/opt/demo/checkpoints/price_stream"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

print("PRICE STREAMING STARTED", flush=True)

query.awaitTermination()
