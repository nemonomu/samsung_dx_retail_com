"""
Best Buy Detail Crawler Test - samsung_sku_name & estimated_annual_electricity_use 테스트
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

    def click_specifications(self):
        """Specification 버튼 클릭"""
        try:
            print("  [INFO] Specification 버튼 클릭...")
            xpaths = [
                "//button[@class='c-button-unstyled specs-accordion font-weight-medium w-full flex justify-content-between align-items-center CiN3vihE2Ub2POwD']",
                "//button[.//h3[text()='Specifications']]",
                "//button[contains(@class, 'specs-accordion')]"
            ]

            for xpath in xpaths:
                try:
                    spec_button = self.driver.find_element(By.XPATH, xpath)
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", spec_button)
                    time.sleep(1)
                    spec_button.click()
                    print("  [OK] Specification 클릭 성공")
                    time.sleep(5)  # 다이얼로그 로딩 대기
                    return True
                except:
                    continue

            print("  [WARNING] Specification 버튼을 찾을 수 없습니다.")
            return False

        except Exception as e:
            print(f"  [ERROR] Specification 클릭 실패: {e}")
            return False

    def extract_samsung_sku_name(self):
        """Samsung_SKU_Name (Model Number) 추출"""
        try:
            print("  [INFO] Samsung SKU Name (Model Number) 추출 중...")

            # Model Number 요소가 나타날 때까지 대기
            try:
                wait = WebDriverWait(self.driver, 10)
                wait.until(EC.presence_of_element_located((By.XPATH, '//div[contains(text(), "Model Number")]')))
                print("  [OK] 다이얼로그 로드 완료")
            except Exception as e:
                print(f"  [WARNING] 다이얼로그 로딩 대기 타임아웃: {e}")

            time.sleep(2)

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            xpaths = [
                '//div[contains(@class, "dB7j8sHUbncyf79K")]//div[contains(text(), "Model Number")]/following-sibling::div[@class="grow basis-none pl-300"]',
                '//li[.//h4[text()="General"]]//div[.//div[text()="Model Number"]]//div[@class="grow basis-none pl-300"]',
                '//div[contains(text(), "Model Number")]/following-sibling::div[@class="grow basis-none pl-300"]',
                '//div[text()="Model Number"]/..//div[@class="grow basis-none pl-300"]',
                '//div[contains(., "Model Number")]//div[contains(@class, "pl-300")]'
            ]

            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    model_number = elem[0].text_content().strip()
                    if model_number:
                        return model_number
            return None

        except Exception as e:
            print(f"  [ERROR] Samsung SKU Name 추출 실패: {e}")
            return None

    def extract_electricity_use(self):
        """Estimated_Annual_Electricity_Use 추출"""
        try:
            print("  [INFO] Estimated Annual Electricity Use 추출 중...")
            time.sleep(2)

            page_source = self.driver.page_source
            tree = html.fromstring(page_source)

            xpaths = [
                '//div[contains(@class, "dB7j8sHUbncyf79K")]//div[contains(text(), "Estimated Annual Electricity Use")]/following-sibling::div[@class="grow basis-none pl-300"]',
                '//li[.//h4[text()="Power"]]//div[.//div[contains(text(), "Estimated Annual Electricity Use")]]//div[@class="grow basis-none pl-300"]',
                '//div[contains(text(), "Estimated Annual Electricity Use")]/following-sibling::div[@class="grow basis-none pl-300"]',
                '//div[contains(text(), "Estimated Annual Electricity Use")]/..//div[@class="grow basis-none pl-300"]',
                '//div[contains(., "Estimated Annual Electricity Use")]//div[contains(@class, "pl-300")]'
            ]

            for xpath in xpaths:
                elem = tree.xpath(xpath)
                if elem:
                    electricity = elem[0].text_content().strip()
                    if electricity:
                        return electricity
            return None

        except Exception as e:
            print(f"  [ERROR] Estimated Annual Electricity Use 추출 실패: {e}")
            return None

    def close_specifications_dialog(self):
        """Specification 다이얼로그 닫기"""
        try:
            print("  [INFO] Specification 다이얼로그 닫기...")
            xpaths = [
                '//button[@data-testid="brix-sheet-closeButton"]',
                '//button[@aria-label="Close Sheet"]',
                '//div[@class="relative"]//button'
            ]

            for xpath in xpaths:
                try:
                    close_button = self.driver.find_element(By.XPATH, xpath)
                    close_button.click()
                    print("  [OK] 다이얼로그 닫기 성공")
                    time.sleep(2)
                    return True
                except:
                    continue

            print("  [WARNING] 다이얼로그 닫기 버튼을 찾을 수 없습니다.")
            return False

        except Exception as e:
            print(f"  [ERROR] 다이얼로그 닫기 실패: {e}")
            return False

    def test_product(self, idx, url):
        """단일 제품 테스트"""
        try:
            print(f"\n{'='*80}")
            print(f"[{idx+1}/{len(TEST_URLS)}] Testing: {url}")
            print(f"{'='*80}")

            # 페이지 접속
            self.driver.get(url)
            time.sleep(random.uniform(8, 12))  # 초기 로딩 대기 증가

            # Specification 버튼 클릭
            if self.click_specifications():
                # Samsung SKU Name 추출
                samsung_sku_name = self.extract_samsung_sku_name()
                print(f"  [RESULT] Samsung_SKU_Name: {samsung_sku_name}")

                # Estimated Annual Electricity Use 추출
                electricity_use = self.extract_electricity_use()
                print(f"  [RESULT] Estimated_Annual_Electricity_Use: {electricity_use}")

                # 다이얼로그 닫기
                self.close_specifications_dialog()
            else:
                print("  [SKIP] Specification 버튼을 찾지 못했습니다.")

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
            print("Testing: samsung_sku_name & estimated_annual_electricity_use")
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
