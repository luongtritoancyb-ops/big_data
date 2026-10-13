
import json
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(
        value, ensure_ascii=False
    ).encode("utf-8")
)

print("=== ECOMMERCE PRICE PRODUCER ===")
print("Nhap exit de thoat.")

try:
    while True:
        product_id = input("\nProduct ID: ").strip()

        if product_id.lower() == "exit":
            break

        platform = input("Platform (Shopee/Tiki/Lazada): ").strip()
        price_text = input("New price (VND): ").strip()

        if not product_id or not platform:
            print("Thieu thong tin san pham.")
            continue

        try:
            price = int(price_text)
            if price <= 0:
                raise ValueError
        except ValueError:
            print("Gia phai la so nguyen duong.")
            continue

        event = {
            "product_id": product_id,
            "platform": platform,
            "price": price,
            "recorded_at": datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        }

        producer.send(
            "product-prices",
            value=event
        ).get(timeout=30)

        print("SENT:", event)

finally:
    producer.flush()
    producer.close()
