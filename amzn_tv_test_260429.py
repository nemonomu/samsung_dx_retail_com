"""
amzn_tv_test_260429.py
- amazon_tv_dt1.extract_detailed_reviews() 패치(3단계 anchor 스크롤) 검증용 테스트 스크립트
- 출력: summarized_review_content / detailed_review_content 만
- DB 저장 없음 (xpaths 미로드 → hardcoded fallback 으로 동작)
"""
import sys
import time
from lxml import html

from amazon_tv_dt1 import AmazonDetailCrawler

# Configure stdout encoding for Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# 테스트 대상 URL
TEST_URLS = [
    'https://www.amazon.com/SAMSUNG-55-Inch-Tracking-Processor-Contour/dp/B0DXN3SDLL/ref=zg_bs_g_172659_d_sccl_82/131-1039182-9245950?th=1',
    'https://www.amazon.com/Sony-Exclusive-Features-PlayStation%C2%AE-K-65XR8B/dp/B0FHXY8DW4/ref=zg_bs_g_172659_d_sccl_86/131-1039182-9245950?th=1',
    'https://www.amazon.com/Roku-Brilliant-Automatic-Brightness-Streaming/dp/B0CLFSWK9V/ref=zg_bs_g_172659_d_sccl_87/131-1039182-9245950?th=1',
    'https://www.amazon.com/VIZIO-Bluetooth-Compatible-Chromecast-VHD32M-0810/dp/B0DFZT5V45/ref=zg_bs_g_172659_d_sccl_88/131-1039182-9245950?psc=1',
    'https://www.amazon.com/VIZIO-D24fM-K01-TBD/dp/B0B286BGSL/ref=zg_bs_g_172659_d_sccl_90/131-1039182-9245950?th=1',
    'https://www.amazon.com/SYLVOX-Weatherproof-Deck-Pro-2-0/dp/B0D22T2LPJ/ref=zg_bs_g_172659_d_sccl_91/131-1039182-9245950?th=1',
    'https://www.amazon.com/Roku-Smart-Screen-Streaming-Bluetooth/dp/B0GPSFJ5Q4/ref=zg_bs_g_172659_d_sccl_92/131-1039182-9245950?psc=1',
    'https://www.amazon.com/FPD-40-inch-Television-Palette-CG40-P3/dp/B0DJR1RWS3/ref=zg_bs_g_172659_d_sccl_95/131-1039182-9245950?th=1',
    'https://www.amazon.com/VIZIO-65-Inch-AirPlay-Chromecast-V655-J09/dp/B092Q8L5DV/ref=zg_bs_g_172659_d_sccl_97/131-1039182-9245950?th=1',
    'https://www.amazon.com/HORION-55-inch-Smart-55P6D-2025/dp/B0DDPSD2B4/ref=zg_bs_g_172659_d_sccl_98/131-1039182-9245950?th=1',
    'https://www.amazon.com/TuTu-Flat-Screen-Television-GoogleTV-Streaming/dp/B0GRTSQSV5/ref=zg_bs_g_172659_d_sccl_99/131-1039182-9245950?th=1',
    'https://www.amazon.com/Roku-1080p-Streaming-Direct-Bluetooth/dp/B0GPS4BB2Q/ref=zg_bs_g_172659_d_sccl_100/131-1039182-9245950?psc=1',
]


def main():
    if not TEST_URLS:
        print("[INFO] TEST_URLS 가 비어있음. 스크립트 상단 TEST_URLS 리스트에 URL 을 채워주세요.")
        return

    crawler = AmazonDetailCrawler()
    crawler.setup_driver()  # 쿠키 로드 포함

    try:
        for idx, url in enumerate(TEST_URLS, 1):
            print("\n" + "=" * 80)
            print(f"[{idx}/{len(TEST_URLS)}] {url}")
            print("=" * 80)

            try:
                crawler.page.get(url)
                time.sleep(5)

                # summarized review (lxml tree 기반)
                tree = html.fromstring(crawler.page.html)
                summarized = crawler.extract_summarized_review(tree)

                # detailed review (방금 패치한 3단계 anchor 스크롤 함수)
                detailed = crawler.extract_detailed_reviews(url)

                print("\n--- RESULT ---")
                print(f"  summarized_review_content: {summarized if summarized else 'NULL'}")
                print(f"  detailed_review_content  : {detailed if detailed else 'NULL'}")

            except Exception as e:
                print(f"  [ERROR] failed for url: {e}")

            time.sleep(3)
    finally:
        try:
            crawler.page.quit()
        except Exception:
            pass


if __name__ == '__main__':
    main()
