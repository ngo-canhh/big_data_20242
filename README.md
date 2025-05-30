# Data Storage and Processing System

## Tổng quan

**Data_storage_and_processing_system** là một hệ thống được xây dựng để thu thập, lưu trữ, xử lý và phân tích dữ liệu giá vàng theo thời gian thực và theo lô (batch). Hệ thống này áp dụng kiến trúc Lambda để đảm bảo khả năng xử lý dữ liệu với độ trễ thấp (real-time) và tính toán toàn diện trên dữ liệu lịch sử (batch).

## Kiến trúc hệ thống

Hệ thống được chia thành các thành phần chính dựa trên kiến trúc Lambda:

1.  **Data Collection (Thu thập dữ liệu):**
    *   Sử dụng một script Python (`crawler/crawl_gold.py`) với Selenium và BeautifulSoup để thu thập dữ liệu giá vàng từ trang web `24h.com.vn`.
    *   Dữ liệu được gửi vào **Apache Kafka** dưới dạng các message JSON.

2.  **Data Storage and Processing (Lưu trữ và Xử lý):**
    *   **Apache Kafka:** Đóng vai trò là message broker, tiếp nhận dữ liệu từ crawler và cung cấp cho các lớp xử lý.
    *   **Batch Layer:**
        *   **Apache Spark (Streaming Job - `kafka_to_hdfs_raw.py`):** Đọc dữ liệu từ Kafka và lưu trữ dữ liệu thô (master dataset) vào **HDFS** dưới định dạng Parquet, được phân vùng theo ngày.(đã ổn)
        *   **Apache Spark (Batch Job - `batch_processor.py`):** Đọc dữ liệu thô từ HDFS, thực hiện tính toán tổng hợp (ví dụ: giá trung bình, min, max hàng ngày) để tạo ra các "batch views". (Tính năng này đang phát triển, chưa hoàn thiện)
    *   **Speed Layer:**
        *   **Apache Spark (Streaming Job - `kafka_to_es_stream.py`):** Đọc dữ liệu gần như real-time từ Kafka, thực hiện một số biến đổi cơ bản và ghi trực tiếp vào Elasticsearch để phục vụ truy vấn nhanh và trực quan hóa bằng Kibana (đã ổn)
    *   **Serving Layer:**
        *   **Elasticsearch:** Lưu trữ cả "batch views" (từ Batch Layer) và dữ liệu gần real-time (từ Speed Layer). Dữ liệu được tổ chức để có thể truy vấn hiệu quả. Elasticsearch sử dụng cơ chế "upsert" để cập nhật batch views.

3.  **Data Query & Analytics (Truy vấn & Phân tích):**
    *   Dữ liệu trong Elasticsearch có thể được truy vấn và phân tích bằng các công cụ như:
        *   **Kibana:** Trực quan hóa dữ liệu, tạo dashboard, khám phá dữ liệu trong Elasticsearch.
        

## Công nghệ sử dụng

*   **Data Ingestion:** Python (Selenium, BeautifulSoup), Apache Kafka
*   **Data Storage:** Apache HDFS (cho master dataset), Elasticsearch (cho serving layer)
*   **Data Processing:** Apache Spark (Spark Streaming, Spark SQL (sẽ phát triển tiếp phần này))
*   **Messaging:** Apache Kafka
*   **Coordination:** Apache Zookeeper (cho Kafka)
*   **Visualization:** Kibana
*   **Containerization:** Docker, Docker Compose

## Cấu trúc thư mục


*   `.`
    *   `crawler/`
        *   `crawl_gold.py`         # Script thu thập dữ liệu giá vàng
    *   `spark-apps/`
        *   `jobs/`
            *   `batch_processor.py`     # Job Spark xử lý batch (HDFS -> ES)
            *   `kafka_to_es_stream.py`  # Job Spark streaming (Kafka -> ES)
            *   `kafka_to_hdfs_raw.py`   # Job Spark streaming (Kafka -> HDFS)
            *   `view_parquet.py`        # (Tùy chọn) Job Spark để xem file Parquet
    *   `docker-compose.yml`        # File định nghĩa hạ tầng Docker
    *   `README.md`                 # File giới thiệu này
    *   `...` (Các file cấu hình hoặc thư mục khác nếu có)

## Hướng dẫn chạy

Xem chi tiết các bước trong tài liệu hướng dẫn riêng (hoặc bạn có thể tóm tắt các bước chính ở đây):

1.  **Cài đặt:** Đảm bảo Docker, Docker Compose và môi trường Python cần thiết đã được cài đặt.
2.  **Khởi động hạ tầng:** Chạy `docker-compose up -d` trong thư mục gốc.
3.  **Kiểm tra Services:** Đảm bảo các container (Kafka, Zookeeper, HDFS, Spark, ES, Kibana) đang chạy và healthy.
4.  **Tạo Kafka Topic:** Chạy lệnh `docker exec kafka-h2dn kafka-topics --create ...` để tạo topic `gold-price-data`.
5.  **Chạy Spark Streaming Jobs:** Sử dụng `docker exec spark-master-h2dn spark-submit --packages ...` để chạy `kafka_to_hdfs_raw.py` và `kafka_to_es_stream.py` trong các terminal riêng biệt.
6.  **Chạy Crawler:** Thiết lập biến môi trường `KAFKA_BROKER_URL=localhost:9092` và chạy `python crawler/crawl_gold.py`. (Có thể chạy ở trong vscode bằng cách RunPythonFile luôn cũng được)
7.  **Dừng hệ thống:** Sử dụng `docker-compose down` (để giữ volumes) hoặc `docker-compose down -v` (để xóa cả volumes).

-- Có một số tính năng hệ thống đang phát triển