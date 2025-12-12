"""
Best Buy Price Test Crawler
- final_sku_price만 수집하여 로그에 출력
- DB 저장 로직 없음
"""
import time
import re
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from lxml import html


class BestBuyPriceTest:
    def __init__(self):
        self.driver = None
        self.test_urls = [
            "https://www.bestbuy.com/product/samsung-75-class-q7f-series-qled-4k-uhd-samsungvision-ai-smart-tizen-tv-2025/J3ZYGXCLLK",
            "https://www.bestbuy.com/product/tcl-58-class-s5-series-4k-uhd-hdr-led-smart-fire-tv-2025/J36QYTH74J",
            "https://www.bestbuy.com/product/samsung-98-class-du9000-series-crystal-uhd-smart-tizen-tv-2024/J3ZYG2T68Y/sku/6578071",
            "https://www.bestbuy.com/product/samsung-77-class-s84d-series-4k-oled-smart-tizen-tv/J3ZYG24ZWJ/sku/6595650/openbox?condition=excellent",
            "https://www.bestbuy.com/product/samsung-65-class-s90f-series-oled-4k-uhd-samsungvision-ai-smart-tizen-tv-2025/J3ZYG2V6WP"
        ]

    def setup_driver(self):
        """Chrome driver setup"""
        try:
            print("[INFO] Setting up Chrome driver...")
            options = uc.ChromeOptions()
            options.page_load_strategy = 'eager'
            self.driver = uc.Chrome(options=options)
            self.driver.set_page_load_timeout(120)
            self.driver.maximize_window()
            print("[OK] Driver setup complete")
            return True
        except Exception as e:
            print(f"[ERROR] Driver setup failed: {e}")
            return False

    def extract_final_sku_price(self, tree):
        """Final SKU Price extraction (현재 판매 가격) - 컨테이너 기반"""
        try:
            # 1단계: 가격 컨테이너 찾기
            container_xpaths = [
                '/html/body/div[5]/div[4]/div[1]',
                '//div[@class="order-2 t3V0AOwowrTfUzPn "]',
                '//div[contains(@class, "order-2")]'
            ]

            price_container = None
            for xpath in container_xpaths:
                containers = tree.xpath(xpath)
                if containers:
                    price_container = containers[0]
                    break

            if price_container is None or len(price_container) == 0:
                print(f"  [WARNING] price container not found")
                return None

            # 2단계: 컨테이너 내부에서만 가격 extraction
            price_xpaths = [
                './/div[@data-testid="price-block-customer-price"]//span',
                './/div[@data-lu-target="customer_price"]//span',
                './/span[@class="font-sans text-default text-style-body-md-400 font-500 text-7 leading-7"]'
            ]

            for xpath in price_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    price = elem[0].text_content().strip()
                    if price and '$' in price:
                        return price

            # Fallback 1: "See price in cart" 패턴 찾기
            see_price_xpaths = [
                './/div[@data-testid="price-restricted-price-tap-for-price"]//span',
                './/span[contains(text(), "See price in cart")]',
                './/div[@data-testid="price-block"]//span[contains(text(), "See price in cart")]'
            ]

            for xpath in see_price_xpaths:
                elem = price_container.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    if "See price in cart" in text:
                        return "See price in cart"

            # Fallback 2: Check for "This item is no longer available in new condition"
            no_longer_available_xpaths = [
                '/html/body/div[5]/div[3]/div[1]/div/div[2]/div[4]',
                '//div[@class="text-danger text-4 font-500 leading-4"]',
                '//div[contains(@class, "text-danger")][contains(text(), "no longer available")]',
                '//div[contains(text(), "This item is no longer available in new condition")]'
            ]

            for xpath in no_longer_available_xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    if "no longer available in new condition" in text.lower():
                        return "This item is no longer available in new condition."

            return None
        except Exception as e:
            print(f"  [ERROR] Final_SKU_Price extraction failed: {e}")
            return None

    def scrape_price(self, url):
        """페이지에서 가격만 수집"""
        try:
            print(f"\n{'='*80}")
            print(f"[URL] {url}")

            self.driver.get(url)

            # 핵심 element load wait
            try:
                wait = WebDriverWait(self.driver, 20)
                wait.until(EC.presence_of_element_located(
                    (By.XPATH, '//h1[contains(@class, "h4") or contains(@class, "heading")]')
                ))
                print(f"  [OK] page load complete")
            except TimeoutException:
                print(f"  [ERROR] page loading timeout")
                return None

            # page 소스 가져오기
            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            # 가격 추출
            final_sku_price = self.extract_final_sku_price(tree)
            print(f"  [PRICE] final_sku_price: {final_sku_price}")

            return final_sku_price

        except Exception as e:
            print(f"  [ERROR] scrape failed: {e}")
            import traceback
            traceback.print_exc()
            return None

    def run(self):
        """메인 실행"""
        try:
            print("="*80)
            print("Best Buy Price Test Crawler")
            print("="*80)

            if not self.setup_driver():
                return

            results = []
            for url in self.test_urls:
                price = self.scrape_price(url)
                results.append({
                    'url': url,
                    'final_sku_price': price
                })
                time.sleep(3)

            # 결과 요약
            print("\n" + "="*80)
            print("RESULTS SUMMARY")
            print("="*80)
            for i, result in enumerate(results, 1):
                print(f"[{i}] {result['url'][:60]}...")
                print(f"    final_sku_price: {result['final_sku_price']}")
            print("="*80)

        except Exception as e:
            print(f"[ERROR] crawler execution error: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
                print("\n[INFO] Driver closed")


def main():
    crawler = BestBuyPriceTest()
    crawler.run()


if __name__ == "__main__":
    main()
