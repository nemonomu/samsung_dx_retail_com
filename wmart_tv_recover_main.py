"""
특정 Main batch 의 누락 URL 복구 전용 스크립트
사용: python wmart_tv_recover_main.py <main1_batch_id> <main2_batch_id>
예:  python wmart_tv_recover_main.py 20260420_130000 20260420_130500

wmart_tv_main_1/2 의 해당 batch URL 중, Walmart_tv_detail_crawled 에
main_rank 로 아직 저장되지 않은 것만 재스크래핑한다.
WalmartDetailCrawler 를 상속하고 load_product_urls 만 교체.

crawl_datetime/calendar_week 은 main1 batch_id (KST) → 로컬 시각으로
변환한 값을 쓰도록 datetime.now() 를 세션별로 frozen 한다.
세션 기준 집계 쿼리에서 복구 레코드가 원본 세션 시각 범위에 포함됨.
"""
import sys
import os
from contextlib import contextmanager
from datetime import datetime
import pytz
import wmart_tv_dt1 as _dt1_module
from wmart_tv_dt1 import WalmartDetailCrawler, Tee


@contextmanager
def _freeze_datetime(target):
    """wmart_tv_dt1 모듈 내 datetime.now() 를 target 으로 고정"""
    original = _dt1_module.datetime

    class _FrozenDatetime(original):
        @classmethod
        def now(cls, tz=None):
            return target

    _dt1_module.datetime = _FrozenDatetime
    try:
        yield
    finally:
        _dt1_module.datetime = original


class RecoverMainCrawler(WalmartDetailCrawler):
    def __init__(self, main1_batch_id, main2_batch_id):
        super().__init__()
        self.target_main1_batch_id = main1_batch_id
        self.target_main2_batch_id = main2_batch_id
        # max_skus 제한 회피: 복구는 모든 누락분 처리해야 함
        self.max_skus = 10 ** 9
        # batch_id (KST) → 로컬 naive datetime 변환 (main1 기준)
        kst = pytz.timezone('Asia/Seoul').localize(
            datetime.strptime(main1_batch_id, '%Y%m%d_%H%M%S')
        )
        self.target_datetime = kst.astimezone().replace(tzinfo=None)
        print(f"[INFO] crawl_datetime/calendar_week 을 {self.target_datetime} 로 고정 (main1 batch 기준)")

    def save_to_db(self, data):
        """부모 save_to_db 를 호출하되 datetime.now() 만 원본 세션 시각으로 치환"""
        with _freeze_datetime(self.target_datetime):
            return super().save_to_db(data)

    def _batch_id_to_local(self, batch_id):
        """KST wall-clock batch_id → 로컬 naive datetime"""
        kst = pytz.timezone('Asia/Seoul').localize(
            datetime.strptime(batch_id, '%Y%m%d_%H%M%S')
        )
        return kst.astimezone().replace(tzinfo=None)

    def load_product_urls(self):
        """지정 세션 시간 윈도우 내에서 main_rank 로 미저장된 URL 로드"""
        try:
            cursor = self.db_conn.cursor()

            # Load from wmart_tv_main_1
            cursor.execute("""
                SELECT main_rank, Product_url,
                       Pick_Up_Availability, Shipping_Availability, Delivery_Availability,
                       SKU_Status, Retailer_Membership_Discounts, Available_Quantity_for_Purchase,
                       Inventory_Status
                FROM wmart_tv_main_1
                WHERE batch_id = %s
                  AND Product_url IS NOT NULL
                  AND Product_url != ''
                ORDER BY main_rank
            """, (self.target_main1_batch_id,))
            main1_rows = cursor.fetchall()
            print(f"[INFO] wmart_tv_main_1 (batch {self.target_main1_batch_id}): {len(main1_rows)} URLs")

            # Load from wmart_tv_main_2
            cursor.execute("""
                SELECT main_rank, Product_url,
                       Pick_Up_Availability, Shipping_Availability, Delivery_Availability,
                       SKU_Status, Retailer_Membership_Discounts, Available_Quantity_for_Purchase,
                       Inventory_Status
                FROM wmart_tv_main_2
                WHERE batch_id = %s
                  AND Product_url IS NOT NULL
                  AND Product_url != ''
                ORDER BY main_rank
            """, (self.target_main2_batch_id,))
            main2_rows = cursor.fetchall()
            print(f"[INFO] wmart_tv_main_2 (batch {self.target_main2_batch_id}): {len(main2_rows)} URLs")

            # Main1/2 source의 모든 unique URL 수집 (with metadata)
            source_urls = {}  # {url: (main_rank, availability_data)}
            for row in main1_rows:
                url = row[1]
                if url not in source_urls:  # 첫 출현만 저장 (main1이 main2보다 우선)
                    source_urls[url] = {
                        'main_rank': row[0],
                        'pick_up_availability': row[2],
                        'shipping_availability': row[3],
                        'delivery_availability': row[4],
                        'sku_status': row[5],
                        'retailer_membership_discounts': row[6],
                        'available_quantity_for_purchase': row[7],
                        'inventory_status': row[8]
                    }
            for row in main2_rows:
                url = row[1]
                if url not in source_urls:  # main2의 고유 URL만 추가
                    source_urls[url] = {
                        'main_rank': row[0],
                        'pick_up_availability': row[2],
                        'shipping_availability': row[3],
                        'delivery_availability': row[4],
                        'sku_status': row[5],
                        'retailer_membership_discounts': row[6],
                        'available_quantity_for_purchase': row[7],
                        'inventory_status': row[8]
                    }

            print(f"[INFO] Total unique Main URLs (main1+main2): {len(source_urls)}")

            # Walmart_tv_detail_crawled 에서 Main source URL들만 필터링
            if source_urls:
                placeholders = ','.join(['%s'] * len(source_urls))
                cursor.execute(f"""
                    SELECT DISTINCT product_url
                    FROM Walmart_tv_detail_crawled
                    WHERE product_url IN ({placeholders})
                """, list(source_urls.keys()))
                saved_urls = {row[0] for row in cursor.fetchall()}
                print(f"[INFO] Already saved in Walmart_tv_detail_crawled: {len(saved_urls)}")
            else:
                saved_urls = set()
                print(f"[WARNING] No source URLs found")

            cursor.close()

            # 누락 URL = source - saved
            missing = []
            for url, metadata in source_urls.items():
                if url not in saved_urls:
                    missing.append({
                        'page_type': 'main',
                        'url': url,
                        'main_rank': metadata['main_rank'],
                        'bsr_rank': None,
                        'pick_up_availability': metadata['pick_up_availability'],
                        'shipping_availability': metadata['shipping_availability'],
                        'delivery_availability': metadata['delivery_availability'],
                        'sku_status': metadata['sku_status'],
                        'retailer_membership_discounts': metadata['retailer_membership_discounts'],
                        'available_quantity_for_purchase': metadata['available_quantity_for_purchase'],
                        'inventory_status': metadata['inventory_status']
                    })

            print(f"[OK] Missing Main URLs to recover: {len(missing)}")
            for item in missing:
                print(f"     Main {item['main_rank']}: {item['url'][:80]}...")
            return missing

        except Exception as e:
            print(f"[ERROR] Failed to load missing URLs: {e}")
            import traceback
            traceback.print_exc()
            return []


if __name__ == "__main__":
    # Auto-find latest main1, main2 batches if not provided
    if len(sys.argv) == 1:
        from config import DB_CONFIG
        import psycopg2
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("SELECT batch_id FROM wmart_tv_main_1 ORDER BY batch_id DESC LIMIT 1")
        row = cursor.fetchone()
        main1_batch_id = row[0] if row else None

        cursor.execute("SELECT batch_id FROM wmart_tv_main_2 ORDER BY batch_id DESC LIMIT 1")
        row = cursor.fetchone()
        main2_batch_id = row[0] if row else None

        cursor.close()
        conn.close()

        if not main1_batch_id or not main2_batch_id:
            print("ERROR: Cannot find main1/main2 batches in database")
            sys.exit(1)
        print(f"[AUTO] Found latest batches: main1={main1_batch_id}, main2={main2_batch_id}")
    elif len(sys.argv) != 3:
        print("Usage: python wmart_tv_recover_main.py [main1_batch_id main2_batch_id]")
        print("  (omit to auto-find latest batches)")
        print("Example: python wmart_tv_recover_main.py 20260420_130000 20260420_130500")
        sys.exit(1)
    else:
        main1_batch_id = sys.argv[1]
        main2_batch_id = sys.argv[2]

    log_dir = "C:\\samsung_dx_retail_com\\log"
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(
        log_dir,
        datetime.now().strftime("%Y%m%d_%H%M%S") + f"_recover_main_{main1_batch_id}_{main2_batch_id}.txt"
    )
    tee = Tee(log_filename)
    sys.stdout = tee

    try:
        print(f"[INFO] Recovery target Main batch_ids: main1={main1_batch_id}, main2={main2_batch_id}")
        crawler = RecoverMainCrawler(main1_batch_id, main2_batch_id)
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print(f"\n[INFO] Recovery finished. Log: {log_filename}")
    sys.stdout = tee.terminal
    tee.close()
