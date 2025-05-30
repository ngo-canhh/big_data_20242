import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
import json
import time
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cấu hình Kafka
KAFKA_BROKER = "172.17.0.2:29092"  # Thay bằng IP thực tế của Kafka
KAFKA_TOPIC = "player-data"
CSV_FILE_PATH = "c:/Users/ADMIN/Documents/2024.2/BIG DATA/Project/spark-apps/data/Cleaned_Data.csv"

# Hàm kiểm tra và tạo topic Kafka
def check_kafka_topic(bootstrap_servers, topic_name):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=[bootstrap_servers])
        topic_list = admin_client.list_topics()
        if topic_name not in topic_list:
            logger.info(f"Topic {topic_name} does not exist. Creating...")
            new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            logger.info(f"Topic {topic_name} created successfully.")
        else:
            logger.info(f"Topic {topic_name} already exists.")
        admin_client.close()
    except Exception as e:
        logger.error(f"ERROR checking/creating Kafka topic: {e}", exc_info=True)
        raise

# Hàm gửi dữ liệu vào Kafka
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

try:
    # Kiểm tra và tạo topic Kafka
    check_kafka_topic(KAFKA_BROKER, KAFKA_TOPIC)

    # Khởi tạo Kafka Producer
    logger.info(f"Connecting to Kafka broker at {KAFKA_BROKER}...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        retries=3,
        acks='all'
    )
    logger.info("Successfully connected to Kafka.")

    # Đọc file CSV
    logger.info(f"Loading CSV file from {CSV_FILE_PATH}...")
    df = pd.read_csv(CSV_FILE_PATH)
    logger.info("CSV file loaded successfully.")
    logger.info(f"Number of records: {len(df)}")

    # Chuyển từng dòng thành JSON và gửi vào Kafka
    successful_sends = 0
    for index, row in df.iterrows():
        try:
            # Chuyển dòng thành dictionary
            record = row.to_dict()
            # Gửi vào Kafka
            if send_to_kafka_async(producer, KAFKA_TOPIC, record):
                successful_sends += 1
                logger.info(f"Successfully sent record {index+1}/{len(df)} to Kafka topic '{KAFKA_TOPIC}'.")
            else:
                logger.error(f"Failed to send record {index+1}/{len(df)} to Kafka topic '{KAFKA_TOPIC}'.")
        except Exception as e:
            logger.error(f"Error processing record {index+1}/{len(df)}: {e}")
            continue

    logger.info(f"Finished sending {successful_sends}/{len(df)} records to Kafka topic '{KAFKA_TOPIC}'.")

except Exception as e:
    logger.error(f"ERROR during CSV to Kafka operation: {e}", exc_info=True)
finally:
    if 'producer' in locals():
        logger.info("Flushing and closing Kafka producer...")
        producer.flush(timeout=60)
        producer.close()
    logger.info("Script finished.")