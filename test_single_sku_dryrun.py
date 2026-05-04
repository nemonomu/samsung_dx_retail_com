"""
Dry-run test for amazon_tv_dt1.py — single URL, no DB INSERT/UPDATE.

목적: shipping_info → delivery_availability + fastest_delivery 분리 수집 검증
  - delivery_primary  xpath → data['delivery_availability']
  - delivery_secondary xpath → data['fastest_delivery']
  - delivery_holiday  xpath → drop (수집 안 함)
  - shipping_info     key   → 더 이상 data dict 에 없어야 정상

Usage (RDP 에서):
    cd C:\\Users\\gomguard\\Documents\\퀵오일\\삼성전자\\samsung_dx_retail_com\\samsung_dx_retail_com
    python -u test_single_sku_dryrun.py

DB INSERT/UPDATE 는 monkey-patch 로 차단됨 (read-only xpath 로드만 DB 호출).
"""

import sys
from amazon_tv_dt1 import AmazonDetailCrawler


TEST_URL = (
    "https://www.amazon.com/Hisense-32-Inch-Class-1080p-32A4NF/dp/B0DYQZFXSN/"
    "ref=sr_1_38?dib=eyJ2IjoiMSJ9.zCEce6W2LXFQC9UXRvhP67sSHTPppKRkm6q3JcSTMT_"
    "uvCujPVGB93ljcyMC9mbzhrQvz1jA994lPm56UjIzCpd_kDoSQINC2OzWNxLnqS3WufLiQP2"
    "GedpweSex5QSuFW7veWd-i---sApewabVX-ipZMrjHE4nHrdcbdXnsdtNvcTTqgL31KzAF1i"
    "GkZnOmH0fyY_6vXXHZhpgqIdD2sttp96_dyBw72XaKCqyhT8.yYXHmpoK6PoNehLCnaZpByv"
    "jqhEb6rsqaSFXyHhZryY&dib_tag=se&keywords=tv&qid=1777824065&xpid=khdVh5qe"
    "glJy2&th=1"
)
TEST_ASIN = "B0DYQZFXSN"


def mock_save_to_db(self, data):
    """Dry-run replacement for AmazonDetailCrawler.save_to_db — print only, no DB write."""
    print("\n" + "=" * 80)
    print("[DRY-RUN] save_to_db() 차단 — INSERT 0건. 추출된 data dict 출력:")
    print("=" * 80)

    # 핵심 검증 필드
    highlight = ['delivery_availability', 'fastest_delivery', 'shipping_info']
    print("\n--- 핵심 검증 필드 (수집 위치 변경 확인) ---")
    for k in highlight:
        v = data.get(k, '<KEY NOT SET>')
        marker = ''
        if k == 'shipping_info' and v != '<KEY NOT SET>':
            marker = '  ⚠ 잔존! (제거되어야 함)'
        elif k in ('delivery_availability', 'fastest_delivery') and v == '<KEY NOT SET>':
            marker = '  ⚠ 미수집!'
        print(f"  {k:25s} = {v!r}{marker}")

    print("\n--- 전체 data dict ---")
    for k in sorted(data.keys()):
        v = data[k]
        v_str = str(v)
        if len(v_str) > 200:
            v_str = v_str[:200] + " ...(truncated)"
        print(f"  {k:40s} = {v_str}")

    print("=" * 80)
    print("[DRY-RUN] mock_save_to_db 정상 종료 (return True)")
    return True


def main():
    # 모든 INSERT/UPDATE 차단 — save_to_db 를 mock 으로 교체
    AmazonDetailCrawler.save_to_db = mock_save_to_db

    crawler = AmazonDetailCrawler()

    try:
        print("=" * 80)
        print("Amazon TV dt1 — Single SKU DRY-RUN Test")
        print(f"URL: {TEST_URL[:80]}...")
        print(f"ASIN: {TEST_ASIN}")
        print("=" * 80)

        # 1. DB 연결 (xpath 로드용 read-only)
        print("\n[STEP 1/4] Connecting to database (read-only for xpath load)...")
        if not crawler.connect_db():
            print("[ERROR] DB connect failed")
            return 1

        # 2. xpath 로드 (amazon_tv_config 에서 SELECT)
        print("\n[STEP 2/4] Loading XPaths from amazon_tv_config...")
        if not crawler.load_xpaths():
            print("[ERROR] xpath load failed")
            return 1

        # 3. Browser setup (cookies 포함)
        print("\n[STEP 3/4] Setting up browser & loading cookies...")
        crawler.setup_driver()

        # 4. 단일 URL detail page 스크래핑
        url_data = {
            'asin': TEST_ASIN,
            'page_type': 'main',
            'url': TEST_URL,
            'main_rank': None,
            'bsr_rank': None,
            'number_of_units_purchased_past_month': None,
        }

        print("\n[STEP 4/4] Scraping detail page (DRY-RUN — DB writes blocked)")
        print("=" * 80)
        success = crawler.scrape_detail_page(url_data)

        print("\n" + "=" * 80)
        print(f"[RESULT] scrape_detail_page returned: {success}")
        print("=" * 80)

        return 0 if success else 1

    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        try:
            if crawler.page:
                crawler.page.quit()
                print("[OK] Browser closed")
        except Exception:
            pass
        try:
            if crawler.db_conn:
                crawler.db_conn.close()
                print("[OK] DB connection closed")
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
