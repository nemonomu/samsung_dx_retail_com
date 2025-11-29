"""
BestBuy TV count_of_star_ratings Test
"""
import time
import re
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml import html


def setup_driver():
    """Chrome driver setup"""
    options = uc.ChromeOptions()
    options.page_load_strategy = 'eager'
    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(120)
    driver.maximize_window()
    return driver


def extract_star_ratings_from_reviews_page(driver):
    """Count_of_Star_Ratings extraction (See All Customer Reviews page에서)"""
    try:
        time.sleep(3)  # page 로딩 wait
        ratings = {}
        # XPath 패턴 (5점부터 1점까지)
        xpaths = [
            '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[1]/div/label/span[5]',  # 5점
            '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[2]/div/label/span[5]',  # 4점
            '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[3]/div/label/span[5]',  # 3점
            '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[4]/div/label/span[5]',  # 2점
            '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[5]/div/label/span[5]'   # 1점
        ]

        print(f"  [DEBUG] Trying to extract star ratings...")

        # 5점부터 1점까지 순서로 extraction
        for idx, xpath in enumerate(xpaths):
            star = 5 - idx  # 5, 4, 3, 2, 1
            try:
                elem = driver.find_element(By.XPATH, xpath)
                count = elem.text.strip()
                print(f"  [DEBUG] {star}star found: {count}")
                # 1star는 단수형, 나머지는 복수형
                key = f"{star}star" if star == 1 else f"{star}stars"
                ratings[key] = count
            except Exception as e:
                print(f"  [DEBUG] {star}star not found: {e}")
                # 찾지 못하면 0으로 설정
                key = f"{star}star" if star == 1 else f"{star}stars"
                ratings[key] = "0"

        # 형식: "5stars:9 4stars:1 3stars:0 2stars:0 1star:2" (공백으로 구분)
        rating_str = " ".join([f"{k}:{v}" for k, v in ratings.items()])
        return rating_str if rating_str else None

    except Exception as e:
        print(f"  [ERROR] Star ratings extraction failed: {e}")
        return None


def navigate_to_reviews_page(driver):
    """리뷰 페이지로 이동"""
    try:
        # "See All Customer Reviews" 버튼 클릭
        review_button_xpaths = [
            '//a[contains(text(), "See All Customer Reviews")]',
            '//button[contains(text(), "See All Customer Reviews")]',
            '//*[contains(@class, "see-all-reviews")]',
            '//a[contains(@href, "reviews")]'
        ]

        for xpath in review_button_xpaths:
            try:
                elem = driver.find_element(By.XPATH, xpath)
                print(f"  [DEBUG] Found review button with xpath: {xpath}")
                driver.execute_script("arguments[0].click();", elem)
                time.sleep(3)
                return True
            except:
                continue

        print("  [WARNING] Review button not found, trying to scroll to reviews section")
        return False

    except Exception as e:
        print(f"  [ERROR] Failed to navigate to reviews page: {e}")
        return False


def main():
    test_urls = [
        "https://www.bestbuy.com/product/sony-65-class-bravia-8-oled-4k-uhd-smart-google-tv-2024/J7XSRH595K/sku/6578577",
        "https://www.bestbuy.com/product/insignia-65-class-f50-series-led-4k-uhd-smart-fire-tv/J2FPJKSFY7",
        "https://www.bestbuy.com/product/hisense-55-class-qd6-series-qled-4k-uhd-hdr-smart-fire-tv-2025/J3Z9Z42SQZ/sku/6621212"
    ]

    driver = None
    try:
        print("=" * 80)
        print("BestBuy count_of_star_ratings Test")
        print("=" * 80)

        driver = setup_driver()
        print("[OK] Driver setup complete")

        for url in test_urls:
            print("\n" + "=" * 80)
            print(f"[URL] {url}")
            print("=" * 80)

            # 페이지 로드
            driver.get(url)
            time.sleep(5)

            # 현재 페이지 HTML 출력 (디버깅용)
            print(f"  [DEBUG] Current URL: {driver.current_url}")

            # 리뷰 페이지로 이동 시도
            navigate_to_reviews_page(driver)

            # count_of_star_ratings 추출
            star_ratings = extract_star_ratings_from_reviews_page(driver)

            print("\n" + "-" * 40)
            print(f"[RESULT] URL: {url}")
            print(f"[RESULT] count_of_star_ratings: {star_ratings}")
            print("-" * 40)

            time.sleep(2)

    except Exception as e:
        print(f"[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if driver:
            driver.quit()
            print("\n[OK] Driver closed")


if __name__ == "__main__":
    main()
