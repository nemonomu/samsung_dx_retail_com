"""
Best Buy Detail Crawler Test - top_mentions 테스트
DB 저장 없이 로그만 출력
"""
import time
import random
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml import html

# 테스트 대상 URLs
TEST_URLS = [
    "https://www.bestbuy.com/product/sony-77-class-bravia-8-oled-4k-uhd-smart-google-tv-2024/J7XSRH5959",
    "https://www.bestbuy.com/product/sony-55-class-bravia-xr-a75l-oled-4k-uhd-smart-google-tv-2023/J7XSRH5LZH",
    "https://www.bestbuy.com/product/sony-43-class-bravia-3-led-4k-uhd-smart-google-tv-2024/J7XSRH5638",
    "https://www.bestbuy.com/product/samsung-58-class-u7900-series-uhd-4k-smart-tizen-tv-2025/J3ZYG2V5VT",
    "https://www.bestbuy.com/product/samsung-55-class-du7200-series-crystal-uhd-4k-smart-tizen-tv-2024/J3ZYG2TX9L/sku/6575355/openbox?condition=excellent",
    "https://www.bestbuy.com/product/roku-50-class-4k-hdr-led-smart-rokutv-2025/J3PFCJQYSX",
    "https://www.bestbuy.com/product/lg-65-class-ur9000-series-led-4k-uhd-smart-webos-tv-2023/JJ8VPZRSZ2",
    "https://www.bestbuy.com/product/lg-65-class-ut70-series-led-4k-uhd-smart-webos-tv-2024/JJ8VPZH54L/sku/6593578/openbox?condition=excellent",
    "https://www.bestbuy.com/product/lg-65-class-ut75-series-led-4k-uhd-smart-webos-tv-2024/JJ8VPZQZ4F",
    "https://www.bestbuy.com/product/lg-70-class-uq75-series-led-4k-uhd-smart-webos-tv-2022/JJ8VPZ6JWS",
    "https://www.bestbuy.com/product/lg-75-class-80-series-qned-4k-uhd-smart-webos-tv-2024/JJ8VPZHXW7/sku/6578180/openbox?condition=excellent",
    "https://www.bestbuy.com/product/lg-75-class-85-series-qned-4k-uhd-smart-webos-tv-2024/JJ8VPZQVV3",
    "https://www.bestbuy.com/product/lg-75-class-ua7000-series-led-ai-4k-uhd-smart-webos-tv-2025/JJ8VPZW4PX",
    "https://www.bestbuy.com/product/lg-75-class-ur9000-series-led-4k-uhd-smart-webos-tv-2023/JJ8VPZRS6S",
    "https://www.bestbuy.com/product/lg-75-class-ut70-series-led-4k-uhd-smart-webos-tv-2024/JJ8VPZHFJZ",
    "https://www.bestbuy.com/product/lg-75-class-ut75-series-led-4k-uhd-smart-webos-tv-2024/JJ8VPZH8XJ",
    "https://www.bestbuy.com/product/lg-77-class-b4-series-oled-4k-uhd-smart-webos-tv-2024/JJ8VPZQF64",
    "https://www.bestbuy.com/product/lg-77-class-c4-series-oled-evo-4k-uhd-smart-webos-tv-2024/JJ8VPZQ5Z7/sku/6578044/openbox?condition=excellent",
    "https://www.bestbuy.com/product/lg-77-class-c5-series-oled-evo-ai-4k-uhd-smart-webos-tv-2025/JJ8VPZTRLJ",
    "https://www.bestbuy.com/product/lg-77-class-g4-series-oled-evo-4k-uhd-smart-webos-tv-with-one-wall-design-2024/JJ8VPZQ5KS",
    "https://www.bestbuy.com/product/lg-83-class-g4-series-oled-evo-4k-uhd-smart-webos-tv-with-one-wall-design-2024/JJ8VPZQFSP",
    "https://www.bestbuy.com/product/lg-86-class-ua77-series-led-ai-4k-uhd-smart-webos-tv-2025/JJ8VPZTRCL",
    "https://www.bestbuy.com/product/lg-86-class-uq75-series-led-4k-uhd-smart-webos-tv-2022/JJ8VPZRL38/sku/6524315/openbox?condition=fair",
    "https://www.bestbuy.com/product/lg-86-class-ut75-series-led-4k-uhd-smart-webos-tv-2024/JJ8VPZH82K/sku/6578185/openbox?condition=excellent",
    "https://www.bestbuy.com/product/lg-98-class-ut90-series-led-4k-uhd-smart-webos-tv-2024/JJ8VPZHZ93",
    "https://www.bestbuy.com/product/pioneer-32-class-led-hd-smart-xumo-tv/J2FPJKS95C",
    "https://www.bestbuy.com/product/pioneer-43-class-led-4k-uhd-smart-xumo-tv/J2FPJKSG7S",
    "https://www.bestbuy.com/product/pioneer-50-class-led-4k-uhd-smart-xumo-tv/J2FPJKSGJ2",
    "https://www.bestbuy.com/product/pioneer-55-class-led-4k-uhd-smart-roku-tv/J2FPJK9CCZ",
    "https://www.bestbuy.com/product/pioneer-55-class-led-4k-uhd-smart-xumo-tv/J2FPJKSGJP",
    "https://www.bestbuy.com/product/lg-65-class-g4-series-oled-evo-4k-uhd-smart-webos-tv-2024/JJ8VPZQ5KW",
    "https://www.bestbuy.com/product/hisense-85-class-u6-series-miniled-qled-uhd-4k-hdr-smart-fire-tv-2025/J3Z9Z42SG3",
    "https://www.bestbuy.com/product/hisense-32-class-a4-series-hd-720p-led-smart-roku-tv-2025/J3Z9Z42R5W"
]

class BestBuyTest:
    def __init__(self):
        self.driver = None

    def setup_driver(self):
        """Chrome 드라이버 설정"""
        try:
            print("[INFO] Chrome 드라이버 설정 중...")
            self.driver = uc.Chrome()
            self.driver.maximize_window()
            print("[OK] 드라이버 설정 완료")
            return True
        except Exception as e:
            print(f"[ERROR] 드라이버 설정 실패: {e}")
            return False

    def click_see_all_reviews(self):
        """See All Customer Reviews 버튼 클릭"""
        try:
            print("  [INFO] See All Customer Reviews 버튼 찾는 중...")
            print("  [INFO] 페이지 스크롤 시작...")

            scroll_height = self.driver.execute_script("return document.body.scrollHeight")
            current_position = 0
            step = 400

            xpaths = [
                '//button[contains(., "See All Customer Reviews")]',
                '//button[contains(@class, "Op9coqeII1kYHR9Q")]'
            ]

            while current_position < scroll_height:
                for xpath in xpaths:
                    try:
                        button = self.driver.find_element(By.XPATH, xpath)
                        print("  [OK] See All Customer Reviews 버튼 발견")
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                        time.sleep(3)  # 대기 시간 증가
                        wait = WebDriverWait(self.driver, 10)
                        wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
                        button.click()
                        print("  [OK] See All Customer Reviews 클릭 성공")
                        time.sleep(5)  # 리뷰 페이지 로딩 대기 증가
                        return True
                    except:
                        continue

                current_position += step
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                time.sleep(2)  # 스크롤 후 대기 시간 증가

            print("  [WARNING] See All Customer Reviews 버튼을 찾을 수 없습니다.")
            return False

        except Exception as e:
            print(f"  [ERROR] See All Customer Reviews 클릭 실패: {e}")
            return False

    def extract_star_ratings(self):
        """Count_of_Star_Ratings 추출"""
        try:
            print("  [INFO] Star ratings 추출 중...")
            time.sleep(3)  # 추가 대기

            ratings = {}
            xpaths = [
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[1]/div/label/span[5]',  # 5점
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[2]/div/label/span[5]',  # 4점
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[3]/div/label/span[5]',  # 3점
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[4]/div/label/span[5]',  # 2점
                '//*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[5]/div/label/span[5]'   # 1점
            ]

            for idx, xpath in enumerate(xpaths):
                star = 5 - idx
                try:
                    elem = self.driver.find_element(By.XPATH, xpath)
                    count = elem.text.strip()
                    key = f"{star}star" if star == 1 else f"{star}stars"
                    ratings[key] = count
                except Exception:
                    key = f"{star}star" if star == 1 else f"{star}stars"
                    ratings[key] = "0"

            rating_str = " ".join([f"{k}:{v}" for k, v in ratings.items()])
            return rating_str if rating_str else None

        except Exception as e:
            print(f"  [ERROR] Star ratings 추출 실패: {e}")
            return None

    def extract_top_mentions(self):
        """Top_Mentions 추출"""
        try:
            print("  [INFO] Top mentions 추출 중...")
            time.sleep(3)  # 추가 대기

            # XPath 패턴 (ID가 동적이므로 class 기반으로 찾기)
            xpaths = [
                # "Highly rated by customers for" 섹션의 span.text-nowrap들
                '//div[contains(@class, "customer-review-pros-stats")]//span[@class="text-nowrap"]',
                # 더 넓은 패턴
                '//div[contains(., "Highly rated by customers for")]//span[@class="text-nowrap"]'
            ]

            mentions = []
            for xpath in xpaths:
                try:
                    elements = self.driver.find_elements(By.XPATH, xpath)
                    if elements:
                        for elem in elements:
                            text = elem.text.strip()
                            # 콤마나 기타 불필요한 문자 제거
                            text = text.replace(',', '').strip()
                            if text:
                                mentions.append(text)
                        break
                except Exception:
                    continue

            if mentions:
                # 첫 번째 항목만 반환 (예: "Picture Quality")
                return mentions[0]

            return None

        except Exception as e:
            print(f"  [ERROR] Top mentions 추출 실패: {e}")
            return None

    def extract_reviews(self):
        """리뷰 20개 수집"""
        try:
            print("  [INFO] Detailed reviews 추출 중...")
            time.sleep(3)  # 추가 대기

            reviews = []
            collected = 0
            page = 1

            while collected < 20:
                page_source = self.driver.page_source
                tree = html.fromstring(page_source)

                review_elements = tree.xpath('//li[@class="review-item"]//div[@class="ugc-review-body"]//p[@class="pre-white-space"]')

                for elem in review_elements:
                    if collected >= 20:
                        break
                    review_text = elem.text_content().strip()
                    if review_text:
                        reviews.append(review_text)
                        collected += 1
                        print(f"    [리뷰 {collected}/20] {review_text[:50]}...")

                if collected >= 20:
                    break

                try:
                    next_button = self.driver.find_element(By.XPATH, '//li[contains(@class, "page next")]//a')
                    print(f"  [INFO] 다음 페이지로 이동 중... (Page {page + 1})")
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                    time.sleep(2)
                    next_button.click()
                    time.sleep(4)  # 페이지 로딩 대기 증가
                    page += 1
                except:
                    print("  [INFO] 다음 페이지 버튼이 없습니다. 수집 종료.")
                    break

            return " | ".join(reviews) if reviews else None

        except Exception as e:
            print(f"  [ERROR] 리뷰 수집 실패: {e}")
            return None

    def test_product(self, idx, url):
        """단일 제품 테스트"""
        try:
            print(f"\n{'='*80}")
            print(f"[{idx+1}/{len(TEST_URLS)}] Testing: {url}")
            print(f"{'='*80}")

            # 페이지 접속
            self.driver.get(url)
            time.sleep(random.uniform(8, 12))  # 초기 로딩 대기 증가

            # See All Customer Reviews 클릭
            if self.click_see_all_reviews():
                # Top mentions 추출만 테스트
                top_mentions = self.extract_top_mentions()
                print(f"  [RESULT] Top_Mentions: {top_mentions}")
            else:
                print("  [SKIP] See All Reviews 버튼을 찾지 못했습니다.")

            return True

        except Exception as e:
            print(f"  [ERROR] 테스트 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def run(self):
        """메인 테스트 실행"""
        try:
            print("="*80)
            print("Best Buy Detail Crawler Test")
            print("Testing: top_mentions only")
            print("="*80)

            # 드라이버 설정
            if not self.setup_driver():
                return

            # 각 URL 테스트
            for idx, url in enumerate(TEST_URLS):
                self.test_product(idx, url)
                time.sleep(random.uniform(3, 5))

            print("\n" + "="*80)
            print(f"테스트 완료! 총 {len(TEST_URLS)}개 URL 테스트")
            print("창을 닫지 않고 로그를 확인하세요.")
            print("="*80)

            # 창 닫지 않기
            input("\n\n[Enter]를 눌러 종료하세요...")

        except Exception as e:
            print(f"[ERROR] 테스트 실행 오류: {e}")
            import traceback
            traceback.print_exc()

        finally:
            if self.driver:
                self.driver.quit()
                print("\n[INFO] 드라이버 종료")

if __name__ == "__main__":
    test = BestBuyTest()
    test.run()
