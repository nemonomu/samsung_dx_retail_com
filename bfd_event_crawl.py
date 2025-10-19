import time
import random
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from lxml import html

# Database configuration
DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

BASE_URL = "https://blackfriday.com"
TARGET_RETAILERS = ["Walmart", "Amazon", "Best Buy"]

class BFDEventCrawler:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.db_conn = None
        self.events_data = {
            'Walmart': [],
            'Amazon': [],
            'Best Buy': []
        }

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            print("[OK] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False

    def setup_driver(self):
        """Setup Chrome WebDriver with page load strategy"""
        chrome_options = Options()

        # Set page load strategy to 'none' - don't wait for full page load
        chrome_options.page_load_strategy = 'none'

        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--lang=en-US,en;q=0.9')

        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False
        }
        chrome_options.add_experimental_option("prefs", prefs)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)

        # With page_load_strategy='none', we control wait times manually
        self.wait = WebDriverWait(self.driver, 30)

        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });
                window.chrome = {
                    runtime: {}
                };
            '''
        })

        print("[OK] WebDriver setup complete (page_load_strategy=none)")

    def get_retailer_containers(self):
        """Get retailer containers from main page"""
        try:
            print(f"\n[INFO] Accessing main page: {BASE_URL}")
            try:
                self.driver.get(BASE_URL)
                print("[OK] Page accessed")
            except Exception as e:
                print(f"[ERROR] Failed to load page: {e}")
                print("[INFO] Trying to continue anyway...")

            print("[INFO] Waiting for page to load...")
            time.sleep(5)
            print("[OK] Wait completed")

            # Light scroll to trigger any lazy loading (15% of page)
            print("[INFO] Scrolling 15% to trigger lazy load...")
            page_height = self.driver.execute_script("return document.body.scrollHeight")
            scroll_to = int(page_height * 0.15)
            self.driver.execute_script(f"window.scrollTo(0, {scroll_to});")
            time.sleep(2)

            print("[INFO] Parsing page source...")
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # Find all base containers
            print("[INFO] Searching for base containers...")
            containers = tree.xpath('//div[@class="flex flex-wrap text-left justify-center"]')
            print(f"[INFO] Found {len(containers)} total containers")

            if len(containers) == 0:
                print("[WARNING] No containers found. Saving screenshot for debugging...")
                self.driver.save_screenshot("bfd_main_page_error.png")
                print("[DEBUG] Screenshot saved to bfd_main_page_error.png")

            retailer_urls = {}

            for idx, container in enumerate(containers, 1):
                # Extract retailer name
                retailer_name_elem = container.xpath('.//span[@class="font-bold"]')
                if not retailer_name_elem:
                    print(f"[DEBUG] Container {idx}: No retailer name found")
                    continue

                retailer_name = retailer_name_elem[0].text_content().strip()
                print(f"[DEBUG] Container {idx}: Found retailer '{retailer_name}'")

                # Check if it's one of our target retailers
                if retailer_name in TARGET_RETAILERS:
                    # Extract URL
                    url_elem = container.xpath('.//a[@class="block relative h-60 md:h-63 lg:h-66"]/@href')
                    if url_elem:
                        relative_url = url_elem[0]
                        full_url = f"{BASE_URL}{relative_url}"
                        retailer_urls[retailer_name] = full_url
                        print(f"[FOUND] {retailer_name}: {full_url}")
                    else:
                        print(f"[WARNING] {retailer_name}: No URL found")
                else:
                    print(f"[SKIP] {retailer_name}: Not a target retailer")

            print(f"\n[SUMMARY] Found {len(retailer_urls)} target retailers")
            return retailer_urls

        except Exception as e:
            print(f"[ERROR] Failed to get retailer containers: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def scrape_retailer_events(self, retailer_name, url):
        """Scrape event schedules from retailer page"""
        max_retries = 2
        for attempt in range(max_retries):
            try:
                print(f"\n[{retailer_name}] Accessing: {url} (Attempt {attempt + 1}/{max_retries})")

                try:
                    self.driver.get(url)
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"[WARNING] Page load failed: {e}")
                        print(f"[INFO] Retrying in 10 seconds...")
                        time.sleep(10)
                        continue
                    else:
                        raise

                print(f"[{retailer_name}] Page loaded, waiting for content...")
                time.sleep(random.uniform(8, 12))

                # Wait for event containers to load
                try:
                    self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "ad-scan-nav-slide")))
                except Exception as e:
                    print(f"[WARNING] Event containers not found via wait, trying anyway...")

                page_source = self.driver.page_source
                tree = html.fromstring(page_source)
                break  # Success, exit retry loop

            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"[ERROR] Attempt {attempt + 1} failed: {e}")
                    print(f"[INFO] Retrying...")
                    time.sleep(10)
                else:
                    print(f"[ERROR] All {max_retries} attempts failed for {retailer_name}")
                    return False

        # Find all event containers
        try:
            event_containers = tree.xpath('//li[contains(@class, "ad-scan-nav-slide")]')
            print(f"[{retailer_name}] Found {len(event_containers)} event containers")

            events = []

            for idx, container in enumerate(event_containers, 1):
                try:
                    # Extract event name
                    event_name_elem = container.xpath('.//span[@class="text-xs leading-1 my-px font-bold line-clamp-2 overflow-hidden"]')
                    if not event_name_elem:
                        print(f"  [{retailer_name}][{idx}] No event name found")
                        continue

                    event_name = event_name_elem[0].text_content().strip()

                    # Extract event period
                    time_elements = container.xpath('.//time/@datetime')
                    if len(time_elements) < 2:
                        print(f"  [{retailer_name}][{idx}] Incomplete date info for {event_name}")
                        continue

                    start_date = time_elements[0].strip()
                    end_date = time_elements[1].strip()

                    # Format dates (e.g., "2024-11-25 00:00:00" -> "Nov 25")
                    start_formatted = self.format_date(start_date)
                    end_formatted = self.format_date(end_date)

                    # Combine into final format: "Event Name: MMM DD-MMM DD"
                    event_schedule = f"{event_name}: {start_formatted}-{end_formatted}"
                    events.append(event_schedule)

                    print(f"  [{retailer_name}][{idx}] {event_schedule}")

                except Exception as e:
                    print(f"  [{retailer_name}][{idx}] Error extracting event: {e}")
                    continue

            self.events_data[retailer_name] = events
            print(f"[{retailer_name}] Collected {len(events)} events")

            return True

        except Exception as e:
            print(f"[ERROR] Failed to scrape {retailer_name} events: {e}")
            import traceback
            traceback.print_exc()
            return False

    def format_date(self, datetime_str):
        """Format datetime string to 'MMM DD' format"""
        try:
            # Parse date like "2024-11-25 00:00:00"
            from datetime import datetime
            dt = datetime.strptime(datetime_str.split()[0], "%Y-%m-%d")
            return dt.strftime("%b %d")
        except Exception as e:
            print(f"[WARNING] Date format error: {e}")
            return datetime_str[:10]  # Return raw date if format fails

    def save_to_db(self):
        """Save event schedules to database"""
        try:
            cursor = self.db_conn.cursor()

            # Combine all events for each retailer into single text
            bestbuy_schedule = " | ".join(self.events_data.get('Best Buy', []))
            walmart_schedule = " | ".join(self.events_data.get('Walmart', []))
            amazon_schedule = " | ".join(self.events_data.get('Amazon', []))

            cursor.execute("""
                INSERT INTO bfd_event_crawl
                (Bestbuy_event_schedule, Walmart_event_schedule, Amazon_event_schedule)
                VALUES (%s, %s, %s)
            """, (bestbuy_schedule, walmart_schedule, amazon_schedule))

            self.db_conn.commit()
            cursor.close()

            print("\n[OK] Data saved to database")
            print(f"  - Best Buy: {bestbuy_schedule or 'N/A'}")
            print(f"  - Walmart: {walmart_schedule or 'N/A'}")
            print(f"  - Amazon: {amazon_schedule or 'N/A'}")

            return True

        except Exception as e:
            print(f"[ERROR] Failed to save to DB: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print("Black Friday Event Schedule Crawler")
            print("="*80)

            # Connect to database
            if not self.connect_db():
                return

            # Setup WebDriver
            self.setup_driver()

            # Get retailer URLs from main page
            retailer_urls = self.get_retailer_containers()

            if not retailer_urls:
                print("[ERROR] No target retailers found")
                return

            # Scrape events for each retailer
            for retailer_name, url in retailer_urls.items():
                self.scrape_retailer_events(retailer_name, url)
                time.sleep(random.uniform(3, 5))

            # Save to database
            self.save_to_db()

            print("\n" + "="*80)
            print("BFD Event Crawling completed!")
            print("="*80)

        except Exception as e:
            print(f"[ERROR] Crawler failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    try:
        crawler = BFDEventCrawler()
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print("\n[INFO] Crawler terminated. Exiting...")
