from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lower, to_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = (SparkSession.builder.appName("EcommerceReviewStreaming")
         .master("local[2]")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("review_id", StringType()),
    StructField("product_id", StringType()),
    StructField("rating", IntegerType()),
    StructField("comment", StringType()),
    StructField("review_time", StringType()),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:19092")
       .option("subscribe", "product-reviews")
       .option("startingOffsets", "earliest")
       .load())

parsed = (raw.select(from_json(col("value").cast("string"), schema).alias("r"))
          .select("r.*")
          .filter(col("review_id").isNotNull() & col("product_id").isNotNull()
                  & col("rating").between(1, 5)))

# Demo: phân loại cảm xúc bằng số sao; chưa phải mô hình NLP từ nội dung review.
processed = (parsed
    .withColumn("review_time", to_timestamp("review_time", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("sentiment", when(col("rating") >= 4, "positive")
                .when(col("rating") <= 2, "negative").otherwise("neutral"))
    .withColumn("processed_at", current_timestamp()))

jdbc_url = "jdbc:postgresql://postgres:5432/ecommerce"
properties = {"user": "demo", "password": "demo123", "driver": "org.postgresql.Driver"}

def save_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    data = batch_df.cache()
    try:
        count = data.count()
        # Lưu lịch sử theo từng batch dưới dạng Parquet.
        data.write.mode("append").parquet("/opt/demo/lake/reviews")
        # Đồng thời ghi PostgreSQL để truy vấn và chứng minh pipeline hoạt động.
        data.write.jdbc(url=jdbc_url, table="reviews", mode="append", properties=properties)
        print(f"BATCH {batch_id}: Đã lưu {count} review vào Parquet + PostgreSQL", flush=True)
    finally:
        data.unpersist()

query = (processed.writeStream
         .foreachBatch(save_batch)
         .option("checkpointLocation", "/opt/demo/checkpoints/review_stream")
         .trigger(processingTime="5 seconds")
         .start())
print("SPARK STREAMING STARTED: đang đọc topic product-reviews", flush=True)
query.awaitTermination()
