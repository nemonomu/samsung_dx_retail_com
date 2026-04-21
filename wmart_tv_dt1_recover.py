"""
특정 BSR batch 의 누락 URL 복구 전용 스크립트
사용: python wmart_tv_dt1_recover.py <bsr_batch_id>
예:  python wmart_tv_dt1_recover.py 20260420_131233

wmart_tv_bsr_crawl 의 해당 batch URL 중, Walmart_tv_detail_crawled 에
bsr_rank 로 아직 저장되지 않은 것만 재스크래핑한다.
WalmartDetailCrawler 를 상속하고 load_product_urls 만 교체.

crawl_datetime/calendar_week 은 bsr_batch_id (KST) → 로컬 시각으로
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


class RecoverCrawler(WalmartDetailCrawler):
    def __init__(self, bsr_batch_id):
        super().__init__()
        self.target_bsr_batch_id = bsr_batch_id
        # max_skus 제한 회피: 복구는 모든 누락분 처리해야 함
        self.max_skus = 10 ** 9
        # batch_id (KST) → 로컬 naive datetime 변환
        kst = pytz.timezone('Asia/Seoul').localize(
            datetime.strptime(bsr_batch_id, '%Y%m%d_%H%M%S')
        )
        self.target_datetime = kst.astimezone().replace(tzinfo=None)
        print(f"[INFO] crawl_datetime/calendar_week 을 {self.target_datetime} 로 고정")

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
        """지정 세션 시간 윈도우 내에서 bsr_rank 로 미저장된 URL 로드"""
        try:
            cursor = self.db_conn.cursor()

            cursor.execute("""
                SELECT bsr_rank, Product_url,
                       Pick_Up_Availability, Shipping_Availability, Delivery_Availability,
                       SKU_Status, Retailer_Membership_Discounts, Available_Quantity_for_Purchase,
                       Inventory_Status
                FROM wmart_tv_bsr_crawl
                WHERE batch_id = %s
                  AND Product_url IS NOT NULL
                  AND Product_url != ''
                ORDER BY bsr_rank
            """, (self.target_bsr_batch_id,))
            rows = cursor.fetchall()
            print(f"[INFO] wmart_tv_bsr_crawl (batch {self.target_bsr_batch_id}): {len(rows)} URLs")

            # 세션 시간 윈도우 계산: [target_session_main1, next_session_main1)
            cursor.execute("""
                SELECT batch_id FROM wmart_tv_main_1
                WHERE batch_id <= %s
                ORDER BY batch_id DESC LIMIT 1
            """, (self.target_bsr_batch_id,))
            row = cursor.fetchone()
            if row:
                session_main1 = row[0]
                session_start = self._batch_id_to_local(session_main1)
            else:
                # main1 기록 없음: bsr 시각 - 1h 를 세션 시작으로 근사
                from datetime import timedelta
                session_main1 = None
                session_start = self.target_datetime - timedelta(hours=1)

            cursor.execute("""
                SELECT batch_id FROM wmart_tv_main_1
                WHERE batch_id > %s
                ORDER BY batch_id ASC LIMIT 1
            """, (session_main1 or self.target_bsr_batch_id,))
            row = cursor.fetchone()
            session_end = self._batch_id_to_local(row[0]) if row else datetime(9999, 12, 31)

            # crawl_datetime 컬럼이 varchar 라 문자열로 비교 (포맷 일치 시 lexicographic == 시간 순서)
            session_start_str = session_start.strftime('%Y-%m-%d %H:%M:%S')
            session_end_str = session_end.strftime('%Y-%m-%d %H:%M:%S')
            print(f"[INFO] Session window: [{session_start_str}, {session_end_str})")

            # "tv_retail_com 에 없는 URL" 이 복구 대상이므로 bsr_rank 조건 없이 URL 존재만 체크
            cursor.execute("""
                SELECT DISTINCT product_url
                FROM tv_retail_com
                WHERE product_url IS NOT NULL
                  AND crawl_datetime >= %s
                  AND crawl_datetime < %s
            """, (session_start_str, session_end_str))
            already_saved = {row[0] for row in cursor.fetchall()}
            print(f"[INFO] Already in tv_retail_com (this session): {len(already_saved)}")

            cursor.close()

            missing = []
            for row in rows:
                url = row[1]
                if url in already_saved:
                    continue
                missing.append({
                    'page_type': 'bsr',
                    'url': url,
                    'main_rank': None,
                    'bsr_rank': row[0],
                    'pick_up_availability': row[2],
                    'shipping_availability': row[3],
                    'delivery_availability': row[4],
                    'sku_status': row[5],
                    'retailer_membership_discounts': row[6],
                    'available_quantity_for_purchase': row[7],
                    'inventory_status': row[8]
                })

            print(f"[OK] Missing BSR URLs to recover: {len(missing)}")
            for item in missing:
                print(f"     BSR {item['bsr_rank']}: {item['url'][:80]}...")
            return missing

        except Exception as e:
            print(f"[ERROR] Failed to load missing URLs: {e}")
            import traceback
            traceback.print_exc()
            return []


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python wmart_tv_dt1_recover.py <bsr_batch_id>")
        print("Example: python wmart_tv_dt1_recover.py 20260420_131233")
        sys.exit(1)

    batch_id = sys.argv[1]

    log_dir = "C:\\samsung_dx_retail_com\\log"
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(
        log_dir,
        datetime.now().strftime("%Y%m%d_%H%M%S") + f"_dt1_recover_{batch_id}.txt"
    )
    tee = Tee(log_filename)
    sys.stdout = tee

    try:
        print(f"[INFO] Recovery target BSR batch_id: {batch_id}")
        crawler = RecoverCrawler(batch_id)
        crawler.run()
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

    print(f"\n[INFO] Recovery finished. Log: {log_filename}")
    sys.stdout = tee.terminal
    tee.close()
