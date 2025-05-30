import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import datetime
import time
import json
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
import random
import os
import logging
import signal

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình ---
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--log-level=3')

URL_TARGET = 'https://www.24h.com.vn/gia-vang-hom-nay-c425.html'
DATE_RANGE_DAYS = 30

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')
KAFKA_TOPIC = 'gold-price-data'

# --- Khởi tạo WebDriver ---
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
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
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

# --- Chuẩn bị danh sách ngày cần crawl ---
today = datetime.date.today()
start_date = today - datetime.timedelta(60)
end_date = today - datetime.timedelta(30)  # Chỉ crawl từ 30 ngày trước đến hôm nay
#lan 2 se crawl tu 30 ngay truoc den hom nay

date_to_crawl_objs = []
curr_date = start_date
while curr_date <= end_date:
    date_to_crawl_objs.append(curr_date)
    curr_date += datetime.timedelta(days=1)

month_names_vn = ['Tháng 1', 'Tháng 2', 'Tháng 3', 'Tháng 4', 'Tháng 5', 'Tháng 6', 'Tháng 7', 'Tháng 8', 'Tháng 9', 'Tháng 10', 'Tháng 11', 'Tháng 12']

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

# --- Vòng lặp Crawl chính ---
logger.info(f"Starting crawl from {start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')}")
successful_sends_attempted = 0
failed_prepares = 0

try:
    logger.info(f"Loading page: {URL_TARGET}")
    driver.get(URL_TARGET)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'txt_date_box_tin_gia_vang')))

    for target_date_obj in date_to_crawl_objs:
        target_month = target_date_obj.month
        target_year = target_date_obj.year
        target_day = target_date_obj.day
        target_month_name_vn = month_names_vn[target_month - 1]
        target_date_str = target_date_obj.strftime("%d/%m/%Y")

        logger.info("-" * 30)
        logger.info(f"Processing date: {target_date_str}")

        date_selection_successful = False
        try:
            date_span = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, 'txt_date_box_tin_gia_vang'))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", date_span)
            driver.execute_script("arguments[0].click();", date_span)
            logger.info("Clicked date span")

            month_select_element = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.ID, 'selectMonth'))
            )
            month_select = Select(month_select_element)
            month_select.select_by_visible_text(target_month_name_vn)
            logger.info(f"Selected month: {target_month_name_vn}")

            year_select_element = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.ID, 'selectYear'))
            )
            year_select = Select(year_select_element)
            year_select.select_by_visible_text(str(target_year))
            logger.info(f"Selected year: {target_year}")

            day_xpath = f"//td[not(contains(@class, 'otherDay')) and contains(@class, 'Day') and normalize-space(text())='{str(target_day)}']"
            target_day_cell = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, day_xpath))
            )
            logger.info(f"Found target day cell for {target_day}")
            driver.execute_script("arguments[0].click();", target_day_cell)
            logger.info(f"Selected date: {target_day}")
            date_selection_successful = True

            # Chờ bảng giá vàng xuất hiện
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'gia-vang-search-data-table'))
            )

        except (TimeoutException, NoSuchElementException, Exception) as e:
            logger.error(f"ERROR selecting date {target_date_str}: {e}")
            continue

        if date_selection_successful:
            try:
                html_content = driver.page_source
                soup = BeautifulSoup(html_content, "html.parser")
                table = soup.find('table', class_='gia-vang-search-data-table')

                if table:
                    tbody = table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')
                        logger.info(f"Found {len(rows)} rows in table for {target_date_str}")
                        row_count_for_date = 0
                        for row_index, row in enumerate(rows):
                            try:
                                cols = row.find_all('td')
                                if len(cols) < 3:
                                    continue

                                loai_vang_tag = cols[0].find('h2')
                                gia_mua_tag = cols[1].find('span', class_='fixW')
                                gia_ban_tag = cols[2].find('span', class_='fixW')

                                if not (loai_vang_tag and gia_mua_tag and gia_ban_tag):
                                    continue

                                loai_vang = loai_vang_tag.text.strip()
                                gia_mua_raw = gia_mua_tag.text.strip()
                                gia_ban_raw = gia_ban_tag.text.strip()

                                if not gia_mua_raw.replace(',', '').isdigit() or not gia_ban_raw.replace(',', '').isdigit():
                                    logger.warning(f"Skipping row {row_index+1} ('{loai_vang}'): Invalid price format ('{gia_mua_raw}', '{gia_ban_raw}')")
                                    continue

                                gia_mua = int(gia_mua_raw.replace(',', ''))
                                gia_ban = int(gia_ban_raw.replace(',', ''))

                                gold_data_record = {
                                    'crawl_timestamp': int(time.time() * 1000),
                                    'price_date': target_date_obj.strftime("%Y-%m-%d"),
                                    'gold_type': loai_vang,
                                    'buy_price': gia_mua,
                                    'sell_price': gia_ban,
                                    'source': URL_TARGET
                                }

                                if send_to_kafka_async(producer, KAFKA_TOPIC, gold_data_record):
                                    successful_sends_attempted += 1
                                    row_count_for_date += 1
                                else:
                                    failed_prepares += 1

                            except Exception as row_e:
                                logger.error(f"ERROR processing row {row_index+1} for date {target_date_str}: {row_e}")
                        logger.info(f"Attempted to send {row_count_for_date} records for date {target_date_str}.")
                    else:
                        logger.warning(f"No tbody found in table for date {target_date_str}")
                else:
                    logger.warning(f"No data table found for date {target_date_str}")

            except Exception as parse_e:
                logger.error(f"ERROR parsing page or sending Kafka for date {target_date_str}: {parse_e}")

        time.sleep(random.uniform(1.5, 3.0))

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