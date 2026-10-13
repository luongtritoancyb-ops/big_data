import argparse
import csv
import json
import time
from kafka import KafkaProducer

parser = argparse.ArgumentParser(description="Phát lại các review CSV vào Kafka")
parser.add_argument("--file", default="data/reviews_demo.csv")
parser.add_argument("--delay", type=float, default=1.0, help="Giây giữa hai review")
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
)
with open(args.file, "r", encoding="utf-8-sig", newline="") as file:
    reader = csv.DictReader(file)
    required = {"review_id", "product_id", "rating", "comment", "review_time"}
    if not required.issubset(reader.fieldnames or []):
        raise ValueError(f"CSV phải có cột: {sorted(required)}")
    for index, record in enumerate(reader, start=1):
        record["rating"] = int(record["rating"])
        producer.send("product-reviews", value=record).get(timeout=30)
        print(f"Đã gửi {index}: {record['review_id']} - {record['comment']}", flush=True)
        time.sleep(args.delay)
producer.flush()
print("Hoàn tất gửi dữ liệu.")
