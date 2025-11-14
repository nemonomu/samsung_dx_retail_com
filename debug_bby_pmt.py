"""
BestBuy Promotion Page HTML 디버깅 스크립트
실제 페이지 HTML을 저장하여 구조 분석
"""
import time
import random
import undetected_chromedriver as uc

def save_page_html():
    driver = None
    try:
        print("[INFO] Chrome 드라이버 설정 중...")
        driver = uc.Chrome()
        driver.maximize_window()

        url = "https://www.bestbuy.com/site/all-tv-home-theater-on-sale/tvs-on-sale/pcmcat1720647543741.c?id=pcmcat1720647543741"
        print(f"[INFO] 페이지 접속: {url}")
        driver.get(url)
        time.sleep(random.uniform(5, 7))

        # HTML 저장
        html_content = driver.page_source
        output_file = "C:/Users/gomguard/samsung_ds/bby_pmt_page.html"

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"[OK] HTML 저장 완료: {output_file}")
        print(f"[INFO] 파일 크기: {len(html_content):,} bytes")

    except Exception as e:
        print(f"[ERROR] 오류 발생: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if driver:
            driver.quit()
            print("[INFO] 드라이버 종료")

if __name__ == "__main__":
    save_page_html()
