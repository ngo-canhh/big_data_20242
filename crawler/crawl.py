import re
import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
import json
import logging
import os
import signal
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException

# --- Cấu hình Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- Cấu hình Kafka ---
KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')
KAFKA_TOPIC = 'football_players'

# --- File paths ---
PATH_TO_LEAGUES = "Leagues.csv"
PATH_TO_CLUBS = "Clubs.csv"
PATH_TO_PLAYERS_LINK = "All_Players_Link.csv"
PATH_TO_FINAL_DATA = "Final_Data.csv"

# --- HTTP session với retry logic ---
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))

# --- Headers cho requests ---
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "accept-language": "en-US,en;q=0.9"
}

# --- Xử lý tín hiệu ngắt ---
producer = None

def signal_handler(sig, frame):
    logger.info("Received interrupt signal. Cleaning up...")
    if producer:
        producer.flush(timeout=60)
        producer.close()
    logger.info("Cleanup completed. Exiting.")
    exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# --- Kiểm tra và tạo Kafka topic ---
def check_and_create_topic():
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER_URL])
        topic_list = admin_client.list_topics()
        if KAFKA_TOPIC not in topic_list:
            logger.info(f"Topic '{KAFKA_TOPIC}' does not exist. Creating...")
            new_topic = NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            logger.info(f"Topic '{KAFKA_TOPIC}' created successfully.")
        else:
            logger.info(f"Topic '{KAFKA_TOPIC}' already exists.")
        admin_client.close()
    except Exception as e:
        logger.error(f"Error checking/creating topic: {e}")
        raise

# --- Hàm gửi dữ liệu vào Kafka (bất đồng bộ) ---
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

# --- Hàm nhập danh sách quốc gia ---
def get_countries():
    logger.info("Nhập danh sách quốc gia (ID và tên). Nhập 'done' khi hoàn tất.")
    countries = []
    while True:
        country_id = input("Nhập CountryID (hoặc 'done' để kết thúc): ")
        if country_id.lower() == 'done':
            break
        country_name = input("Nhập tên quốc gia: ")
        try:
            countries.append({'CountryID': int(country_id), 'Country': country_name})
        except ValueError:
            logger.error("CountryID phải là số. Vui lòng nhập lại.")
    countries_df = pd.DataFrame(countries)
    if countries_df.empty:
        logger.warning("Không có quốc gia nào được nhập.")
    return countries_df

# --- Hàm thu thập đường dẫn giải đấu ---
def scrape_leagues(countries):
    league_name, league_url, league_id = [], [], []
    for i in range(len(countries)):
        url = f"https://www.transfermarkt.com/wettbewerbe/national/wettbewerbe/{countries.loc[i,'CountryID']}"
        logger.info(f"Đang thu thập giải đấu từ {countries.loc[i,'Country']}...")
        try:
            time.sleep(2)
            page = session.get(url, headers=headers, timeout=10)
            page.raise_for_status()
            soup = BeautifulSoup(page.content, 'html.parser')
            
            league_spans = soup.select('.inline-table a')[1:3]  # Lấy hai giải đấu hàng đầu
            for span in league_spans:
                href = span.get('href')
                league_name.append(span.get('title'))
                league_url.append('https://www.transfermarkt.com' + href + '/plus/?saison_id=')
                league_id_match = re.search(r'/wettbewerb/([A-Z0-9]+)$', href)
                league_id.append(league_id_match.group(1) if league_id_match else None)
        except RequestException as e:
            logger.error(f"Lỗi khi thu thập giải đấu từ {countries.loc[i,'Country']}: {e}")
            continue
    
    leagues = pd.DataFrame({'league_name': league_name, 'league_url': league_url, 'league_id': league_id})
    leagues.to_csv(PATH_TO_LEAGUES, index=False, encoding='utf-8-sig')
    logger.info(f"Đã lưu {len(leagues)} giải đấu vào {PATH_TO_LEAGUES}")
    return leagues

# --- Hàm thu thập đường dẫn CLB ---
def scrape_clubs(leagues):
    club_name, club_url = [], []
    for _, row in leagues.iterrows():
        league = row['league_name']
        base_url = row['league_url']
        for season in range(2024, 2025):  # Chỉ lấy mùa 2024
            url = base_url + str(season)
            logger.info(f"Đang thu thập CLB từ {league} mùa {season}...")
            try:
                time.sleep(2)
                page = session.get(url, headers=headers, timeout=10)
                page.raise_for_status()
                soup = BeautifulSoup(page.content, 'html.parser')
                
                club_links = soup.select("#yw1 .no-border-links a:nth-child(1)")
                for link in club_links:
                    club_name.append(link.get('title'))
                    club_url.append('https://www.transfermarkt.com' + link.get('href'))
            except RequestException as e:
                logger.error(f"Lỗi khi thu thập CLB từ {league}: {e}")
                continue
    
    clubs = pd.DataFrame({'club_name': club_name, 'club_url': club_url})
    clubs.to_csv(PATH_TO_CLUBS, index=False, encoding='utf-8-sig')
    logger.info(f"Đã lưu {len(clubs)} CLB vào {PATH_TO_CLUBS}")
    return clubs

# --- Hàm thu thập đường dẫn cầu thủ từ CLB (không dùng phân trang) ---
def scrape_players(clubs):
    player_links = set()
    for _, row in clubs.iterrows():
        club_url = row['club_url']
        club_name = row['club_name']
        logger.info(f"Đang thu thập cầu thủ từ {club_name}...")
        try:
            time.sleep(2)
            response = session.get(club_url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            players_list = soup.select(".inline-table .hauptlink > a")
            for p in players_list:
                link = p.get('href')
                if link:
                    player_links.add(link)

            logger.info(f"Collected {len(player_links)} unique player links so far for {club_name}.")
        except RequestException as e:
            logger.error(f"Failed to crawl {club_name}: {e}")
            continue

    if player_links:
        df_links = pd.DataFrame(list(player_links), columns=["0"])
        df_links.to_csv(PATH_TO_PLAYERS_LINK, index=False, encoding='utf-8-sig')
        logger.info(f"Saved {len(player_links)} unique player links to {PATH_TO_PLAYERS_LINK}")
    else:
        logger.warning("No player links collected.")

    return list(player_links)

# --- Hàm thu thập dữ liệu cầu thủ và thống kê ---
def scrape_player_and_stats(url):
    try:
        pattern = r"/(\d+)$"
        match = re.search(pattern, url)
        if not match:
            logger.error(f"Invalid URL format: {url}")
            return None
        player_id = match.group(1)
        data = {"player_id": player_id}

        page = session.get(url, headers=headers, timeout=10)
        page.raise_for_status()
        soup = BeautifulSoup(page.content, "html.parser")

        try:
            data["name"] = soup.select_one('h1[class="data-header__headline-wrapper"]').text.split("\n")[-1].strip()
        except (AttributeError, IndexError):
            logger.error(f"Failed to extract name for player ID {player_id}")
            return None

        try:
            data["player_club"] = soup.select_one("span[class='data-header__club']").text.strip()
        except (AttributeError, IndexError):
            logger.error(f"Failed to extract club for player {data['name']}")
            return None

        try:
            age_text = soup.select_one('li[class="data-header__label"]').text.split("\n")[-2].split()[-1].strip("()")
            data["age"] = float(age_text)
        except (AttributeError, ValueError, IndexError):
            logger.error(f"Failed to extract age for player {data['name']}")
            return None

        try:
            data["position"] = soup.find('dd', class_='detail-position__position').text.strip()
        except (AttributeError, IndexError):
            logger.error(f"Failed to extract position for player {data['name']}")
            data["position"] = None

        if data["position"] == "Goalkeeper":
            logger.info(f"Skipping player {data['name']} (ID: {player_id}) because they are a goalkeeper")
            return None

        try:
            market_value = soup.select_one('a[class="data-header__market-value-wrapper"]').text.split(" ")[0].replace('€', '')
            if "m" in market_value:
                data["market_value"] = float(market_value.replace("m", "")) * 1000
            elif "k" in market_value:
                data["market_value"] = float(market_value.replace("k", ""))
            else:
                data["market_value"] = float(market_value)
        except (AttributeError, ValueError, IndexError):
            logger.error(f"Failed to extract market value for player {data['name']}")
            return None

        try:
            data["nationality"] = soup.find('span', itemprop="nationality").text.strip()
        except (AttributeError, IndexError):
            logger.error(f"Failed to extract nationality for player {data['name']}")
            return None

        try:
            height_text = re.search(r"Height:.*?([0-9].*?)\n", soup.text, re.DOTALL).group(1).strip().split(" ")[0].replace(",", ".")
            data["player_height"] = float(height_text)
        except (AttributeError, ValueError, IndexError):
            logger.error(f"Failed to extract height for player {data['name']}")
            return None

        try:
            data["player_agent"] = re.search(r"Agent:.*?([A-z].*?)\n", soup.text, re.DOTALL).group(1).strip()
        except (AttributeError, IndexError):
            data["player_agent"] = "None"
        except ValueError:
            logger.error(f"Failed to extract agent for player {data['name']}")
            return None

        try:
            name_span = soup.find_all('span', class_='info-table__content info-table__content--regular', string=lambda text: text and 'name' in text.lower())
            data["strong_foot"] = soup.select('span[class="info-table__content info-table__content--bold"]')[6 if name_span else 5].text.strip()
        except (AttributeError, IndexError):
            logger.error(f"Failed to extract strong foot for player {data['name']}")
            return None

        try:
            contract_text = re.search(r"Contract expires: (.*)", soup.text).group(1).split()[-1]
            data["contract_value_time"] = float(contract_text) if contract_text.isdigit() else contract_text
        except (AttributeError, IndexError):
            logger.error(f"Failed to extract contract expiry for player {data['name']}")
            return None
        except ValueError:
            data["contract_value_time"] = "None"

        name_hyphenated = url.split('com/')[-1].split('/profil')[0].replace(' ', '-')
        stat_url = f"https://www.transfermarkt.com/{name_hyphenated}/leistungsdatendetails/spieler/{player_id}/plus/1?saison=2024&verein=&liga=&wettbewerb=&pos=&trainer_id="
        time.sleep(2)
        stat_page = session.get(stat_url, headers=headers, timeout=10)
        stat_page.raise_for_status()
        stat_soup = BeautifulSoup(stat_page.content, "html.parser")

        def extract_stat(index, default=0):
            try:
                value = stat_soup.find_all("td", {"class": "zentriert"})[index].text
                return float(value) if value != "-" else default
            except (ValueError, AttributeError, IndexError):
                return None

        try:
            minutes_per_goal = stat_soup.find_all("td", {"class": "rechts"})[1].text.split("'")[0]
            minutes_per_goal = float(minutes_per_goal) if minutes_per_goal != "-" else 0
        except (ValueError, AttributeError, IndexError):
            minutes_per_goal = None

        try:
            minutes_played = stat_soup.find_all("td", {"class": "rechts"})[2].text.split("'")[0]
            minutes_played = float(minutes_played) if minutes_played != "-" else 0
        except (ValueError, AttributeError, IndexError):
            minutes_played = None

        data.update({
            "goalkeeper_or_not": '0',
            "appearances": extract_stat(1),
            "PPG": extract_stat(2),
            "goals": extract_stat(3),
            "assists": extract_stat(4),
            "own_goals": extract_stat(5),
            "substitutions_on": extract_stat(6),
            "substitutions_off": extract_stat(7),
            "yellow_cards": extract_stat(8),
            "second_yellow_cards": extract_stat(9),
            "red_cards": extract_stat(10),
            "penalty_goals": extract_stat(11),
            "minutes_per_goal": minutes_per_goal,
            "minutes_played": minutes_played,
            "goals_conceded": 0.0,
            "clean_sheet": 0.0
        })

        data['crawl_timestamp'] = int(time.time() * 1000)
        data['source'] = url
        return data
    except RequestException as e:
        logger.error(f"Request failed for URL {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error for URL {url}: {e}")
        return None

# --- Hàm chính ---
def main():
    # Nhập danh sách quốc gia
    countries = get_countries()
    if countries.empty:
        logger.error("Không có quốc gia nào được nhập. Thoát chương trình.")
        return

    # Thu thập đường dẫn giải đấu
    leagues = scrape_leagues(countries)
    if leagues.empty:
        logger.error("Không thu thập được giải đấu nào. Thoát chương trình.")
        return

    # Thu thập đường dẫn CLB
    clubs = scrape_clubs(leagues)
    if clubs.empty:
        logger.error("Không thu thập được CLB nào. Thoát chương trình.")
        return

    # Thu thập đường dẫn cầu thủ
    player_links = scrape_players(clubs)
    if not player_links:
        logger.error("Không thu thập được liên kết cầu thủ nào. Thoát chương trình.")
        return

    # Kiểm tra và tạo Kafka topic
    check_and_create_topic()

    # Khởi tạo Kafka producer
    try:
        logger.info(f"Connecting to Kafka broker at {KAFKA_BROKER_URL}...")
        global producer
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER_URL],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            retries=5,
            acks='all'
        )
        logger.info("Successfully connected to Kafka.")
    except Exception as e:
        logger.error(f"CRITICAL: Error connecting to Kafka: {e}")
        return

    # Khởi tạo DataFrame cho backup
    columns = [
        "player_id", "name", "player_club", "age", "position", "goalkeeper_or_not",
        "market_value", "nationality", "player_height", "player_agent", "strong_foot",
        "contract_value_time", "appearances", "PPG", "goals", "assists", "own_goals",
        "substitutions_on", "substitutions_off", "yellow_cards", "second_yellow_cards",
        "red_cards", "penalty_goals", "minutes_per_goal", "minutes_played",
        "goals_conceded", "clean_sheet", "crawl_timestamp", "source"
    ]
    final_data = pd.DataFrame(columns=columns)

    # Biến đếm số lần gửi Kafka
    successful_sends_attempted = 0
    failed_prepares = 0

    try:
        # Xử lý từng cầu thủ
        for i, link in enumerate(player_links):
            url = f"https://www.transfermarkt.com{link}"
            logger.info(f"Scraping player {i+1}/{len(player_links)}: {url}")
            player_data = scrape_player_and_stats(url)
            if player_data is None:
                logger.warning(f"Skipping player at index {i} due to scraping error or goalkeeper")
                continue

            # Gửi dữ liệu vào Kafka
            if send_to_kafka_async(producer, KAFKA_TOPIC, player_data):
                successful_sends_attempted += 1
                logger.info(f"Successfully sent data for player ID {player_data['player_id']} to Kafka")
            else:
                failed_prepares += 1
                logger.error(f"Failed to send data for player ID {player_data['player_id']} to Kafka")

            # Thêm vào DataFrame
            final_data = pd.concat([final_data, pd.DataFrame([player_data])], ignore_index=True)

            # Lưu vào CSV sau mỗi 10 cầu thủ
            if (i + 1) % 10 == 0:
                final_data.to_csv(PATH_TO_FINAL_DATA, index=False, encoding='utf-8-sig')
                producer.flush()
                logger.info(f"Saved {i+1} players to {PATH_TO_FINAL_DATA}")

            time.sleep(2)

    finally:
        logger.info("-" * 30)
        logger.info("Crawl loop finished.")
        logger.info(f"Total Kafka send attempts prepared: {successful_sends_attempted}")
        logger.info(f"Total Kafka send preparation failures: {failed_prepares}")

        # Lưu dữ liệu lần cuối
        final_data.to_csv(PATH_TO_FINAL_DATA, index=False, encoding='utf-8-sig')
        logger.info(f"Final data saved to {PATH_TO_FINAL_DATA}")

        # Flush và đóng producer
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

        logger.info("Script finished.")

if __name__ == "__main__":
    main()
    # country = pd.DataFrame([
    #     {'CountryID': 189, 'Country': 'England'},
    #     {'CountryID': 40, 'Country': 'Germany'},
    #     {'CountryID': 75, 'Country': 'Italy'},
    #     {'CountryID': 50, 'Country': 'France'},
    #     {'CountryID': 157, 'Country': 'Spain'}
    # ])