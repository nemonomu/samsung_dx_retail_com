import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from lxml import html
import psycopg2

# Import database configuration
from config import DB_CONFIG

def debug_page(page_number=7):
    """Debug a specific page to see what containers are found"""
    try:
        # Load XPath from database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT xpath FROM xpath_selectors
            WHERE mall_name = 'Amazon' AND data_field = 'base_container'
        """)
        base_xpath = cursor.fetchone()[0]

        cursor.execute("""
            SELECT url FROM page_urls
            WHERE mall_name = 'Amazon' AND page_number = %s
        """, (page_number,))
        url = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        print("="*80)
        print(f"Debugging Page {page_number}")
        print("="*80)
        print(f"URL: {url[:80]}...")
        print(f"Base XPath: {base_xpath}")

        # Setup driver
        chrome_options = Options()
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

        # Access page
        print("\n[INFO] Accessing page...")
        driver.get(url)
        time.sleep(5)

        # Parse with lxml
        page_source = driver.page_source
        tree = html.fromstring(page_source)

        # Find containers
        containers = tree.xpath(base_xpath)
        print(f"\n[INFO] Found {len(containers)} total containers with base XPath")

        # Try alternative XPath - all divs with data-component-type="s-search-result"
        alt_xpath = '//div[@data-component-type="s-search-result"]'
        alt_containers = tree.xpath(alt_xpath)
        print(f"[INFO] Found {len(alt_containers)} containers with alternative XPath")

        # Analyze each container
        print("\n" + "="*80)
        print("Container Analysis:")
        print("="*80)

        for idx, container in enumerate(containers[:20], 1):
            cel_widget = container.get('cel_widget_id', '')
            component_type = container.get('data-component-type', '')
            data_index = container.get('data-index', '')
            data_asin = container.get('data-asin', '')

            # Check if excluded
            is_excluded = False
            if 'loom-desktop' in cel_widget:
                is_excluded = True
                reason = "loom-desktop"
            elif 'sb-themed' in cel_widget:
                is_excluded = True
                reason = "sb-themed"
            elif 'multi-brand' in cel_widget:
                is_excluded = True
                reason = "multi-brand"
            elif component_type == 's-messaging-widget':
                is_excluded = True
                reason = "messaging-widget"

            status = f"EXCLUDED ({reason})" if is_excluded else "VALID"

            print(f"\n[{idx}] {status}")
            print(f"    ASIN: {data_asin}")
            print(f"    data-index: {data_index}")
            print(f"    cel_widget_id: {cel_widget[:60]}")
            print(f"    component_type: {component_type}")

        driver.quit()

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import sys
    page_num = int(sys.argv[1]) if len(sys.argv) > 1 else 7
    debug_page(page_num)
    input("\nPress Enter to exit...")
