import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(
        value, ensure_ascii=False
    ).encode("utf-8")
)

with open("data/prices_demo.csv", encoding="utf-8") as file:
    reader = csv.DictReader(file)

    for row in reader:
        row["price"] = int(row["price"])

        producer.send(
            "product-prices",
            value=row
        ).get(timeout=30)

        print(
            f"Sent: {row['product_id']} - "
            f"{row['platform']} - {row['price']}"
        )

        time.sleep(1)

producer.flush()
producer.close()