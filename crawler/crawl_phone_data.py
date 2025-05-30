import requests
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import json
import os
import logging
import signal
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--log-level=3')

URL_TARGET = 'https://www.thegioididong.com/dtdd'
KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')
KAFKA_TOPIC = 'phone-product-data'

# --- Khởi tạo WebDriver và Kafka Producer ---
driver = None
producer = None

def signal_handler(sig, frame):
    logger.info("Received interrupt signal. Cleaning up...")
    if producer:
        producer.flush(timeout=60)
        producer.close()
    if driver:
        driver.quit()
    logger.info("Cleanup completed. Exiting.")
    exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

try:
    logger.info("Initializing WebDriver...")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.implicitly_wait(5)
    logger.info("WebDriver initialized.")
except Exception as e:
    logger.error(f"CRITICAL: Error initializing WebDriver: {e}")
    exit()

# --- Kiểm tra và tạo Kafka Topic ---
def ensure_kafka_topic():
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER_URL])
        topic_list = admin_client.list_topics()
        if KAFKA_TOPIC not in topic_list:
            logger.info(f"Topic {KAFKA_TOPIC} does not exist. Creating...")
            new_topic = NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            logger.info(f"Topic {KAFKA_TOPIC} created successfully.")
        else:
            logger.info(f"Topic {KAFKA_TOPIC} already exists.")
        admin_client.close()
    except Exception as e:
        logger.error(f"ERROR ensuring Kafka topic: {e}")
        raise

ensure_kafka_topic()

# --- Khởi tạo Kafka Producer ---
try:
    logger.info(f"Connecting to Kafka broker at {KAFKA_BROKER_URL}...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER_URL],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        retries=5,
        acks='all'
    )
    logger.info("Successfully connected to Kafka.")
except Exception as e:
    logger.error(f"CRITICAL: Error connecting to Kafka: {e}")
    if driver:
        driver.quit()
    exit()

# --- Hàm gửi dữ liệu vào Kafka ---
def on_send_success(record_metadata):
    pass

def on_send_error(excp):
    logger.error(f"ERROR sending message to Kafka: {excp}")

def send_to_kafka_async(producer, topic, data, max_retries=3):
    if not data or not producer:
        return False
    for attempt in range(max_retries):
        try:
            producer.send(topic, value=data).add_callback(on_send_success).add_errback(on_send_error)
            return True
        except Exception as e:
            logger.error(f"Attempt {attempt+1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"Failed to send after {max_retries} attempts.")
                return False

# --- Danh sách lưu trữ dữ liệu sản phẩm ---
all_products_data = []
successful_sends_attempted = 0
failed_prepares = 0

try:
    # Truy cập trang web
    logger.info(f"Loading page: {URL_TARGET}")
    driver.get(URL_TARGET)

    # Chờ danh sách sản phẩm xuất hiện
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, 'listproduct'))
    )

    # Nhấn nút "Xem thêm" cho đến khi không còn nút này
    logger.info("Nhấn nút 'Xem thêm' để tải hết sản phẩm...")
    while True:
        try:
            # Tìm nút "Xem thêm"
            view_more_button = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.CLASS_NAME, 'see-more-btn'))
            )
            # Cuộn đến nút "Xem thêm" để đảm bảo nó có thể được nhấn
            driver.execute_script("arguments[0].scrollIntoView(true);", view_more_button)
            time.sleep(5)  # Đợi một chút để tránh lỗi
            view_more_button.click()
            logger.info("Đã nhấn nút 'Xem thêm', chờ tải thêm sản phẩm...")
            time.sleep(5)  # Chờ dữ liệu mới tải
        except:
            logger.info("Không còn nút 'Xem thêm', đã tải hết sản phẩm.")
            break

    # Lấy nội dung HTML sau khi tải hết sản phẩm
    html_content = driver.page_source
    soup = BeautifulSoup(html_content, "html.parser")

    # Tìm danh sách sản phẩm
    product_list = soup.find('ul', class_='listproduct')
    if not product_list:
        logger.error("Không tìm thấy danh sách sản phẩm!")
        driver.quit()
        exit()

    # Lặp qua từng sản phẩm
    products = product_list.find_all('li', class_='item ajaxed __cate_42')
    logger.info(f"Tìm thấy {len(products)} sản phẩm.")

    for index, product in enumerate(products):
        product_data = {}

        # Lấy thẻ <div class="main-contain"> chứa thông tin chính
        main_contain = product.find('a', class_='main-contain')
        if not main_contain:
            logger.warning(f"Bỏ qua sản phẩm {index + 1}: Không tìm thấy thẻ main-contain")
            continue

        # 1. Tên sản phẩm (trong thẻ <h3>)
        name_tag = main_contain.find('h3')
        if name_tag:
            new_model_tag = name_tag.find('span', class_='newModel')
            if new_model_tag:
                new_model_tag.decompose()
            product_data['name'] = name_tag.text.strip() if name_tag else "N/A"
        else:
            product_data['name'] = "N/A"

        # Tách tên sản phẩm thành 3 phần: brand, model, capacity
        full_name = name_tag.text.strip() if name_tag else "N/A"
        if full_name != "N/A":
            name_parts = full_name.split()  # Chia chuỗi thành danh sách các từ
            if len(name_parts) >= 3:  # Đảm bảo có ít nhất 3 từ để tách
                product_data['brand'] = name_parts[0]  # Từ đầu tiên là tên hãng
                product_data['capacity'] = name_parts[-1]  # Từ cuối cùng là dung lượng
                product_data['model'] = " ".join(name_parts[1:-1])  # Phần ở giữa là tên máy

                capa = name_parts[-1]
                if '/' in capa:
                    # Nếu có dấu "/", tách ra và lấy phần đầu tiên
                    capacity_parts = capa.split('/')
                    product_data['RAM'] = capacity_parts[0].strip()
                    product_data['ROM'] = capacity_parts[1].strip() if len(capacity_parts) > 1 else "N/A"
                else:
                    product_data['RAM'] = "N/A"
                    product_data['ROM'] = "N/A"
            else:
                # Nếu không đủ từ, đặt các giá trị mặc định
                product_data['brand'] = name_parts[0] if len(name_parts) > 0 else "N/A"
                product_data['model'] = "N/A"
                product_data['capacity'] = name_parts[-1] if len(name_parts) > 1 else "N/A"
                product_data['RAM'] = "N/A"
                product_data['ROM'] = "N/A"
        else:
            product_data['brand'] = "N/A"
            product_data['model'] = "N/A"
            product_data['capacity'] = "N/A"
            product_data['RAM'] = "N/A"
            product_data['ROM'] = "N/A"

        # 2. Thông tin màn hình (trong thẻ <div class="item-compare gray-bg">)
        screen_info = main_contain.find('div', class_='item-compare gray-bg')
        if screen_info:
            screen_details = screen_info.find_all('span')  # Lấy tất cả thẻ <span> bên trong
            product_data['screen_size'] = screen_details[0].text.strip() if len(screen_details) > 0 else "N/A"
            product_data['resolution'] = screen_details[1].text.strip() if len(screen_details) > 1 else "N/A"
        else:
            product_data['screen_size'] = "N/A"
            product_data['resolution'] = "N/A"

        # 3. Giá sản phẩm
        new_price = main_contain.find('strong', class_='price')
        if new_price:
            new_price = new_price.text.strip()
            new_price = ''.join(filter(str.isdigit, new_price))
            product_data['new_price'] = new_price if new_price else "N/A"
        else:
            product_data['new_price'] = "N/A"
        
        box_p = main_contain.find('div', class_='box-p')
        if box_p:
            old_price = box_p.find('p', class_='price-old')
            if old_price:
                old_price = old_price.text.strip()
                old_price = ''.join(filter(str.isdigit, old_price))
                product_data['old_price'] = old_price if old_price else "N/A"
            else:
                product_data['old_price'] = "N/A"
            
            percent = box_p.find('span', class_='percent')
            if percent:
                percent = percent.text.strip()
                product_data['percent'] = percent if percent else "N/A"
            else:
                product_data['percent'] = "N/A"
        else:
            product_data['old_price'] = "N/A"
            product_data['percent'] = "N/A"

        # 4. Thông tin đánh giá (trong thẻ <div class="rating_Compare has_compare has_quantity">)
        rating_div = product.find('div', class_='rating_Compare has_compare has_quantity')
        if rating_div:
            num_star = rating_div.find('b')
            numstar = num_star.text.strip()
            product_data['rating'] = numstar

            toltal_sell = rating_div.find('span')
            if toltal_sell:
                totalcell = toltal_sell.text.split()
                n = len(totalcell)
                totalCell = totalcell[n - 1]
                # Chuyển đổi số lượng bán nếu có "k"
                if 'k' in totalCell.lower():
                    # Loại bỏ "k", thay dấu "," thành ".", nhân với 1000
                    number = totalCell.lower().replace('k', '').replace(',', '.')
                    totalCell = str(int(float(number) * 1000))
                product_data['sold'] = totalCell
            else:
                product_data['sold'] = "0"
        else:
            product_data['rating'] = "Chưa có đánh giá"
            product_data['sold'] = "N/A"

        # Thêm thông tin crawl_timestamp và source
        product_data['crawl_timestamp'] = int(time.time() * 1000)
        product_data['source'] = URL_TARGET

        # Gửi dữ liệu vào Kafka
        if send_to_kafka_async(producer, KAFKA_TOPIC, product_data):
            successful_sends_attempted += 1
            logger.info(f"Đã gửi sản phẩm {index + 1} vào Kafka: {product_data['name']}")
        else:
            failed_prepares += 1
            logger.error(f"Không thể gửi sản phẩm {index + 1} vào Kafka: {product_data['name']}")

finally:
    logger.info("-" * 30)
    logger.info("Crawl loop finished.")
    logger.info(f"Total Kafka send attempts prepared: {successful_sends_attempted}")
    logger.info(f"Total Kafka send preparation failures: {failed_prepares}")
    if producer:
        logger.info("Flushing Kafka producer (waiting for pending messages)...")
        try:
            producer.flush(timeout=60)
            logger.info("Kafka producer flushed.")
        except Exception as flush_e:
            logger.error(f"ERROR during producer flush: {flush_e}")
        finally:
            logger.info("Closing Kafka producer.")
            producer.close()
    if driver:
        logger.info("Closing WebDriver.")
        driver.quit()
    logger.info("Script finished.")