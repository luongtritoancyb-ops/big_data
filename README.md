# Demo Big Data: Kafka → Spark Structured Streaming → Parquet + PostgreSQL

> Dataset `reviews_demo.csv` là **dữ liệu mô phỏng**, KHÔNG phải bình luận thực từ Shopee/Tiki/Lazada. Thay bằng CSV dữ liệu thật nếu có quyền sử dụng, giữ đúng cột dữ liệu.
>
> Sentiment dùng **rating** làm nhãn quy ước để demo pipeline, KHÔNG phải phân tích NLP nội dung tiếng Việt. Demo cũng chưa có phát hiện review ảo/dự báo/giá hay xử lý exactly-once qua hai đích lưu trữ.

## 0. Yêu cầu
- Windows 10/11, Docker Desktop đang mở (Linux containers), WSL2.
- Python 3.10+, VS Code, kết nối internet lần chạy đầu.
- Khuyến nghị cấp 5–6 GB RAM cho Docker.
- PowerShell ở thư mục gốc dự án (nơi có `docker-compose.yml`).

## 1. Cài package Python
```powershell
py -m pip install -r requirements.txt
```

## 2. Khởi động hệ thống
```powershell
docker compose up -d
docker compose ps
```
Đợi Kafka và PostgreSQL khởi động; kiểm tra logs nếu cần:
```powershell
docker compose logs kafka --tail 30
docker compose logs postgres --tail 30
```

## 3. Tạo topic
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic product-reviews --partitions 1 --replication-factor 1
```

## 4. Terminal A: chạy Spark streaming
```powershell
docker compose exec spark /opt/spark/bin/spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.postgresql:postgresql:42.7.4 /opt/demo/app/streaming_job.py
```
Lần đầu Spark tải các thư viện Kafka / PostgreSQL từ Maven, cần internet và có thể mất thêm thời gian. Giữ terminal mở.

## 5. Terminal B: gửi review mô phỏng
```powershell
py app/producer.py --file data/reviews_demo.csv --delay 1
```
Nếu thấy `Đã gửi 1...` → producer đang gửi các event vào Kafka.

## 6. Terminal C: xem kết quả thực tế trong PostgreSQL
```powershell
docker compose exec postgres psql -U demo -d ecommerce -c "SELECT sentiment, COUNT(*) FROM reviews GROUP BY sentiment ORDER BY sentiment;"
docker compose exec postgres psql -U demo -d ecommerce -c "SELECT review_id, product_id, rating, sentiment FROM reviews ORDER BY review_id LIMIT 20;"
```
Kiểm tra folder `lake/reviews` có tệp `.parquet`; đây là lưu trữ file trên ổ đĩa host, **không phải HDFS hoặc MinIO**.

## 7. Demo streaming thay đổi theo luồng
- Terminal A vẫn chạy Spark.
- Terminal B phát tiếp `py app/producer.py --file data/reviews_demo.csv --delay 1`.
- Terminal C truy vấn lại `SELECT COUNT(*) FROM reviews;` để thấy số bản ghi tăng.
- Dữ liệu test gửi lặp lại sẽ có `review_id` trùng; không sử dụng minh chứng này để khẳng định đã deduplicate.

## 8. Tắt
```powershell
docker compose down
```
**Không dùng `docker compose down -v`** nếu muốn giữ dữ liệu PostgreSQL.

## Trục trặc thường gặp
- `docker is not recognized`: chưa cài Docker Desktop hoặc chưa mở terminal mới.
- `Cannot connect to Docker daemon`: mở Docker Desktop, chờ engine hoạt động.
- `NoBrokersAvailable`: đợi Kafka khởi động và thử lại.
- `Connection refused` ở PostgreSQL: chờ DB khởi động rồi thử lại.
- Spark lỗi Maven / `Failed to find data source: kafka`: kiểm tra internet và lệnh `--packages` đầy đủ.
- Port 9092/5432 đang được dùng: tắt phần mềm dùng port đó trước.
- Nếu thư mục `lake` không ghi được, kiểm tra Docker Desktop chia sẻ ổ đĩa và quyền ghi.

## Những điểm được chứng minh
1. Producer đọc CSV và phát từng event vào topic Kafka.
2. Spark `readStream` đọc Kafka liên tục và xử lý micro-batch.
3. Spark lưu Parquet và JDBC PostgreSQL.
4. Query SQL và log cho thấy dữ liệu đi hết pipeline.

## Giới hạn kỹ thuật
- Tính liên tục ở đây là streaming từ Kafka, dữ liệu nguồn được replay từ CSV chứ không phải cập nhật Shopee thời gian thực.
- Việc ghi hai đích lần lượt qua `foreachBatch` có thể tạo bản ghi trùng khi lỗi/retry; để sản xuất cần idempotent upsert, transaction/batch marker hoặc thiết kế sink phù hợp.
- Chưa có web dashboard, phân tích review tiếng Việt bằng PhoBERT, phát hiện gian lận hay dự báo xu hướng.
