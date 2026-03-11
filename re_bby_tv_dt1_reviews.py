"""
bby_tv_dt1 Review Recovery Script
- detailed_review_content, top_mentions, summarized_review_content NULL 복구
- count_of_reviews > 0인데 위 컬럼이 NULL인 레코드 대상
- Bazaarvoice API로 리뷰 데이터 직접 조회 → bby_tv_crawl + tv_retail_com UPDATE

사용법:
    python re_bby_tv_dt1_reviews.py                          # 날짜 입력 모드
    python re_bby_tv_dt1_reviews.py 2026-03-11               # 특정 날짜
    python re_bby_tv_dt1_reviews.py 2026-03-10 2026-03-11    # 날짜 범위
"""
import time
import re
import sys
import requests
import psycopg2
from datetime import datetime
import pytz
from DrissionPage import ChromiumPage, ChromiumOptions

from config import DB_CONFIG
from bby_config_loader import get_config


class BbyTvReviewRecovery:
    def __init__(self):
        self.page = None
        self.db_conn = None
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.config = get_config()
        self.file_name = 'bby_tv_dt1'
        self.bv_passkey = None  # Bazaarvoice API passkey (페이지에서 1회 추출)

    def connect_db(self):
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_conn.autocommit = True
            print("[OK] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False

    def setup_browser(self):
        try:
            print("[INFO] Setting up browser...")
            co = ChromiumOptions()
            co.no_imgs(True)
            self.page = ChromiumPage(co)
            print("[OK] Browser ready")
            return True
        except Exception as e:
            print(f"[ERROR] Browser setup failed: {e}")
            return False

    def get_null_review_records(self, date_from, date_to):
        """count_of_reviews > 0인데 detailed_review_content가 NULL인 레코드 조회 (URL별 최신 1건)"""
        query = """
        SELECT DISTINCT ON (product_url)
            product_url, crawl_datetime, retailer_sku_name, item, count_of_reviews
        FROM bby_tv_crawl
        WHERE account_name = 'Bestbuy'
          AND crawl_datetime >= %s
          AND crawl_datetime < %s
          AND CAST(NULLIF(REPLACE(count_of_reviews, ',', ''), '') AS INTEGER) > 0
          AND detailed_review_content IS NULL
        ORDER BY product_url, crawl_datetime DESC
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(query, (f"{date_from} 00:00:00", f"{date_to} 23:59:59"))
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            cursor.close()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            print(f"[ERROR] NULL review records query failed: {e}")
            return None

    def get_summary(self, date_from, date_to):
        """날짜 범위의 리뷰 NULL 요약 조회"""
        query = """
        SELECT
            COUNT(*) as total_count,
            SUM(CASE WHEN CAST(NULLIF(REPLACE(count_of_reviews, ',', ''), '') AS INTEGER) > 0
                      AND detailed_review_content IS NULL THEN 1 ELSE 0 END) as review_null_count,
            SUM(CASE WHEN CAST(NULLIF(REPLACE(count_of_reviews, ',', ''), '') AS INTEGER) > 0
                      AND top_mentions IS NULL THEN 1 ELSE 0 END) as mentions_null_count,
            SUM(CASE WHEN CAST(NULLIF(REPLACE(count_of_reviews, ',', ''), '') AS INTEGER) > 0
                      AND detailed_review_content IS NULL THEN 1 ELSE 0 END) as retail_review_null_count
        FROM bby_tv_crawl
        WHERE account_name = 'Bestbuy'
          AND crawl_datetime >= %s
          AND crawl_datetime < %s
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(query, (f"{date_from} 00:00:00", f"{date_to} 23:59:59"))
            row = cursor.fetchone()
            cursor.close()
            return row
        except Exception as e:
            print(f"[ERROR] Summary query failed: {e}")
            return None

    def extract_bv_passkey(self):
        """페이지 소스에서 Bazaarvoice API passkey 추출 (1회만)"""
        if self.bv_passkey:
            return self.bv_passkey

        try:
            page_html = self.page.html
            # BV 설정에서 passkey 추출 (다양한 패턴 시도)
            patterns = [
                r'"passkey"\s*:\s*"([a-zA-Z0-9]+)"',
                r'passkey=([a-zA-Z0-9]+)',
                r"'passkey'\s*:\s*'([a-zA-Z0-9]+)'",
                r'apiKey["\']?\s*[:=]\s*["\']([a-zA-Z0-9]+)["\']',
            ]
            for pattern in patterns:
                match = re.search(pattern, page_html)
                if match:
                    self.bv_passkey = match.group(1)
                    print(f"    [OK] BV passkey found: {self.bv_passkey[:8]}...")
                    return self.bv_passkey

            # JS로 BV 설정 직접 추출 시도
            bv_key = self.page.run_js('''
                try {
                    if (window.$BV && window.$BV.Internal && window.$BV.Internal.config) {
                        return window.$BV.Internal.config.passKey || null;
                    }
                    if (window.BV && window.BV.options) {
                        return window.BV.options.passkey || null;
                    }
                } catch(e) {}
                return null;
            ''')
            if bv_key:
                self.bv_passkey = bv_key
                print(f"    [OK] BV passkey from JS: {self.bv_passkey[:8]}...")
                return self.bv_passkey

            print("    [WARNING] BV passkey not found in page source")
            return None
        except Exception as e:
            print(f"    [ERROR] BV passkey extraction failed: {e}")
            return None

    def extract_numeric_sku(self, product_url):
        """URL 또는 페이지에서 numeric SKU 추출"""
        # 1. URL에서 직접 추출 (/sku/1234567 또는 /1234567)
        match = re.search(r'/(\d{7,})(?:\?|$|#)', product_url)
        if match:
            return match.group(1)

        # 2. 페이지 소스에서 추출
        try:
            page_html = self.page.html
            match = re.search(r'mbo-entrypoint-(\d+)', page_html)
            if match:
                return match.group(1)
            match = re.search(r'"skuId"\s*:\s*"(\d+)"', page_html)
            if match:
                return match.group(1)
            match = re.search(r'SKU[:\s]+(\d{7,})', page_html)
            if match:
                return match.group(1)
        except:
            pass
        return None

    def fetch_reviews_from_bv_api(self, sku_id):
        """Bazaarvoice API로 리뷰 데이터 직접 조회"""
        if not self.bv_passkey:
            print("    [ERROR] No BV passkey available")
            return None

        url = "https://api.bazaarvoice.com/data/reviews.json"
        params = {
            'apiversion': '5.4',
            'passkey': self.bv_passkey,
            'Filter': f'ProductId:{sku_id}',
            'Include': 'Products',
            'Stats': 'Reviews',
            'Limit': '20',
            'Sort': 'SubmissionTime:desc'
        }

        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                print(f"    [ERROR] BV API HTTP {resp.status_code}")
                return None

            data = resp.json()

            if data.get('HasErrors'):
                errors = data.get('Errors', [])
                print(f"    [ERROR] BV API error: {errors}")
                return None

            return data
        except Exception as e:
            print(f"    [ERROR] BV API request failed: {e}")
            return None

    def fetch_reviews_via_browser(self, sku_id):
        """브라우저 내에서 BV API fetch (CORS 우회)"""
        if not self.bv_passkey:
            return None

        api_url = (
            f"https://api.bazaarvoice.com/data/reviews.json"
            f"?apiversion=5.4&passkey={self.bv_passkey}"
            f"&Filter=ProductId:{sku_id}"
            f"&Include=Products&Stats=Reviews&Limit=20"
            f"&Sort=SubmissionTime:desc"
        )

        try:
            data = self.page.run_js(f'''
                var xhr = new XMLHttpRequest();
                xhr.open("GET", "{api_url}", false);
                xhr.send();
                if (xhr.status === 200) {{
                    return JSON.parse(xhr.responseText);
                }}
                return null;
            ''')
            return data
        except Exception as e:
            print(f"    [ERROR] Browser BV fetch failed: {e}")
            return None

    def parse_bv_response(self, bv_data):
        """Bazaarvoice API 응답에서 리뷰 데이터 파싱"""
        result = {
            'detailed_review_content': None,
            'top_mentions': None,
            'summarized_review_content': None,
            'recommendation_intent': None
        }

        if not bv_data:
            return result

        # 1. Reviews → detailed_review_content
        reviews_list = bv_data.get('Results', [])
        if reviews_list:
            formatted = []
            for i, review in enumerate(reviews_list[:20], 1):
                text = review.get('ReviewText', '').strip()
                if text:
                    formatted.append(f"review{i} - {text}")
            if formatted:
                result['detailed_review_content'] = ' ||| '.join(formatted)

        total_reviews = bv_data.get('TotalResults', len(reviews_list))
        content_len = len(result['detailed_review_content']) if result['detailed_review_content'] else 0
        print(f"    [OK] BV Reviews: {len(reviews_list)} fetched (total: {total_reviews}), {content_len} chars")

        # 2. Top Mentions (TagDimensions에서 pros/cons 추출)
        includes = bv_data.get('Includes', {})
        products = includes.get('Products', {})
        mentions = []
        for product_id, product_data in products.items():
            # ReviewStatistics에서 TagDistribution 추출
            review_stats = product_data.get('ReviewStatistics', {})
            tag_dist = review_stats.get('TagDistribution', {})
            for tag_group, tag_values in tag_dist.items():
                if isinstance(tag_values, list):
                    for tv in tag_values:
                        if isinstance(tv, dict):
                            label = tv.get('Label') or tv.get('Id', '')
                            if label:
                                mentions.append(label)
                elif isinstance(tag_values, dict):
                    for tag_id, tag_info in tag_values.items():
                        label = tag_info.get('Label', tag_id) if isinstance(tag_info, dict) else tag_id
                        mentions.append(label)

        # 리뷰 자체의 TagDimensions에서도 추출
        if not mentions:
            for review in reviews_list[:5]:
                tag_dims = review.get('TagDimensions', {})
                for dim_name, dim_data in tag_dims.items():
                    if isinstance(dim_data, dict):
                        values = dim_data.get('Values', [])
                        for v in values:
                            mentions.append(v)
                    elif isinstance(dim_data, list):
                        mentions.extend(dim_data)

        if mentions:
            # 중복 제거 + 순서 유지
            seen = set()
            unique_mentions = []
            for m in mentions:
                if m not in seen:
                    seen.add(m)
                    unique_mentions.append(m)
            result['top_mentions'] = ', '.join(unique_mentions)
        print(f"    [OK] Top_Mentions: {result['top_mentions'] if result['top_mentions'] else 'None'}")

        # 3. Recommendation Intent
        for product_id, product_data in products.items():
            review_stats = product_data.get('ReviewStatistics', {})
            rec_pct = review_stats.get('RecommendedCount')
            total_rec = review_stats.get('TotalReviewCount')
            if rec_pct is not None and total_rec and total_rec > 0:
                pct = round(rec_pct / total_rec * 100)
                result['recommendation_intent'] = f"{pct}% would recommend to a friend"
                break

        print(f"    [OK] Recommendation_Intent: {result['recommendation_intent']}")

        # 4. Summarized Review Content (BV API에는 AI 요약이 없을 수 있음)
        # 없으면 None 유지
        print(f"    [OK] Summarized_Review: (not available via BV API)")

        return result

    def extract_page_review_data(self):
        """페이지 DOM에서 top_mentions, summarized_review, recommendation 추출 (fallback)"""
        data = self.page.run_js('''
            var result = {};

            // Top Mentions (pros/cons data-feature-name)
            var mentions = [];
            document.querySelectorAll('.pros-container button[data-feature-name]').forEach(function(el) {
                mentions.push(el.getAttribute('data-feature-name'));
            });
            document.querySelectorAll('.cons-container button[data-feature-name]').forEach(function(el) {
                mentions.push(el.getAttribute('data-feature-name'));
            });
            result.topMentions = mentions;

            // Recommendation Intent
            var recElem = document.querySelector('.recommendation-percent');
            result.recommendationIntent = recElem ? recElem.textContent.trim() : null;

            // Summarized Review Content
            var sumElem = document.querySelector('p.mb-200.mt-none')
                || document.querySelector('#reviews-accordion p.mb-200');
            result.summarizedReview = sumElem ? sumElem.textContent.trim() : null;

            return result;
        ''')

        result = {}
        if data:
            mentions = data.get('topMentions', [])
            if mentions:
                result['top_mentions'] = ', '.join(mentions)

            rec = data.get('recommendationIntent')
            if rec:
                result['recommendation_intent'] = f"{rec} would recommend to a friend"

            result['summarized_review_content'] = data.get('summarizedReview')

        return result

    def update_db(self, product_url, crawl_datetime, data):
        """bby_tv_crawl + tv_retail_com UPDATE"""
        try:
            cursor = self.db_conn.cursor()
            updated = 0

            # bby_tv_crawl UPDATE
            detail_table = self.config.get_table('detail_data') or 'bby_tv_crawl'
            cursor.execute(f"""
                UPDATE {detail_table}
                SET detailed_review_content = %s,
                    top_mentions = %s,
                    recommendation_intent = %s
                WHERE product_url = %s
                  AND crawl_datetime = %s
                  AND account_name = 'Bestbuy'
            """, (
                data['detailed_review_content'],
                data['top_mentions'],
                data['recommendation_intent'],
                product_url,
                crawl_datetime
            ))
            updated += cursor.rowcount

            # tv_retail_com UPDATE
            retail_com_table = self.config.get_table('retail_com') or 'tv_retail_com'
            cursor.execute(f"""
                UPDATE {retail_com_table}
                SET detailed_review_content = %s,
                    top_mentions = %s,
                    summarized_review_content = %s,
                    recommendation_intent = %s
                WHERE product_url = %s
                  AND crawl_datetime = %s
                  AND account_name = 'Bestbuy'
            """, (
                data['detailed_review_content'],
                data['top_mentions'],
                data['summarized_review_content'],
                data['recommendation_intent'],
                product_url,
                crawl_datetime
            ))
            updated += cursor.rowcount

            cursor.close()
            return updated
        except Exception as e:
            print(f"    [ERROR] DB update failed: {e}")
            return 0

    def run(self, date_from=None, date_to=None):
        print("=" * 80)
        print("  bby_tv_dt1 Review Recovery Script (Bazaarvoice API)")
        print("  - detailed_review_content, top_mentions, summarized_review_content NULL 복구")
        print("=" * 80)

        if not self.connect_db():
            return

        if not date_from:
            date_from = input("Start date (YYYY-MM-DD): ").strip()
        if not date_to:
            date_to = date_from

        print(f"\n[INFO] Recovery range: {date_from} ~ {date_to}")

        # 요약 조회
        summary = self.get_summary(date_from, date_to)
        if not summary:
            print("[ERROR] Summary query failed")
            return

        total, review_null, mentions_null, retail_review_null = summary
        print(f"[INFO] Total records: {total}")
        print(f"[INFO] Detailed_Review NULL (with reviews > 0): {review_null}")
        print(f"[INFO] Top_Mentions NULL (with reviews > 0): {mentions_null}")

        # NULL 레코드 조회
        null_records = self.get_null_review_records(date_from, date_to)
        if not null_records:
            print("[INFO] No recovery targets found. Done.")
            return

        print(f"\n[INFO] Recovery targets: {len(null_records)} URLs")
        for i, rec in enumerate(null_records[:5]):
            print(f"  [{i+1}] {rec['retailer_sku_name'][:60]}... | reviews: {rec['count_of_reviews']}")
        if len(null_records) > 5:
            print(f"  ... and {len(null_records) - 5} more")

        confirm = input(f"\nProceed? (y/n): ").strip().lower()
        if confirm != 'y':
            print("Cancelled.")
            return

        if not self.setup_browser():
            return

        success_count = 0
        fail_count = 0

        try:
            for i, rec in enumerate(null_records):
                url = rec['product_url']
                print(f"\n[{i+1}/{len(null_records)}] {rec['retailer_sku_name'][:60]}...")
                print(f"  URL: {url[:80]}...")

                # 1. 제품 페이지 접근 (SKU + BV passkey 추출용)
                try:
                    self.page.get(url)
                    time.sleep(3)
                except Exception as e:
                    print(f"    [ERROR] Page load failed: {e}")
                    fail_count += 1
                    continue

                # 2. numeric SKU 추출
                numeric_sku = self.extract_numeric_sku(url)
                if not numeric_sku:
                    print("    [ERROR] Could not extract numeric SKU")
                    fail_count += 1
                    continue
                print(f"    [OK] Numeric SKU: {numeric_sku}")

                # 3. BV passkey 추출 (첫 제품에서 1회만)
                if not self.bv_passkey:
                    self.extract_bv_passkey()

                # 4. Bazaarvoice API로 리뷰 조회
                bv_data = self.fetch_reviews_from_bv_api(numeric_sku)

                # API 실패 시 브라우저 내 fetch로 재시도
                if not bv_data:
                    print("    [INFO] Retrying via browser fetch...")
                    bv_data = self.fetch_reviews_via_browser(numeric_sku)

                if not bv_data:
                    print(f"    [FAIL] BV API failed for SKU {numeric_sku}")
                    fail_count += 1
                    time.sleep(2)
                    continue

                # 5. API 응답 파싱
                data = self.parse_bv_response(bv_data)

                # 6. 페이지 DOM에서 추가 데이터 보충 (top_mentions, summarized_review 등)
                if not data['top_mentions'] or not data['summarized_review_content']:
                    # 리뷰 탭으로 이동하여 DOM 데이터 추출 시도
                    reviews_url = f"{url}/sku/{numeric_sku}#tabbed-customerreviews"
                    try:
                        self.page.get(reviews_url)
                        time.sleep(5)
                        page_data = self.extract_page_review_data()
                        if not data['top_mentions'] and page_data.get('top_mentions'):
                            data['top_mentions'] = page_data['top_mentions']
                            print(f"    [OK] Top_Mentions from DOM: {data['top_mentions']}")
                        if not data['summarized_review_content'] and page_data.get('summarized_review_content'):
                            data['summarized_review_content'] = page_data['summarized_review_content']
                            print(f"    [OK] Summarized_Review from DOM: {data['summarized_review_content'][:50]}")
                        if not data['recommendation_intent'] and page_data.get('recommendation_intent'):
                            data['recommendation_intent'] = page_data['recommendation_intent']
                    except:
                        pass

                if not data['detailed_review_content']:
                    print(f"    [FAIL] detailed_review_content still NULL")
                    fail_count += 1
                    time.sleep(2)
                    continue

                # 7. DB UPDATE (같은 URL의 해당 날짜 범위 레코드 모두)
                cursor = self.db_conn.cursor()
                detail_table = self.config.get_table('detail_data') or 'bby_tv_crawl'

                cursor.execute(f"""
                    SELECT crawl_datetime FROM {detail_table}
                    WHERE product_url = %s
                      AND crawl_datetime >= %s
                      AND crawl_datetime < %s
                      AND account_name = 'Bestbuy'
                      AND detailed_review_content IS NULL
                """, (url, f"{date_from} 00:00:00", f"{date_to} 23:59:59"))
                null_dts = [row[0] for row in cursor.fetchall()]
                cursor.close()

                total_updated = 0
                for dt in null_dts:
                    updated = self.update_db(url, dt, data)
                    total_updated += updated

                if total_updated > 0:
                    print(f"    [DB] Updated {total_updated} rows ({len(null_dts)} crawl_datetime(s))")
                    success_count += 1
                else:
                    print(f"    [DB] No rows updated")
                    fail_count += 1

                time.sleep(2)

        except KeyboardInterrupt:
            print("\n[INFO] Interrupted by user")
        except Exception as e:
            print(f"[ERROR] Recovery failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if self.page:
                self.page.quit()
                print("[INFO] Browser closed")

        print(f"\n{'='*80}")
        print(f"[Recovery Result]")
        print(f"  Range: {date_from} ~ {date_to}")
        print(f"  Total targets: {len(null_records)} URLs")
        print(f"  Success: {success_count}")
        print(f"  Fail: {fail_count}")
        print(f"{'='*80}")

        if self.db_conn:
            self.db_conn.close()
            print("[INFO] Database disconnected")
        print("Done.")


def main():
    recovery = BbyTvReviewRecovery()

    if len(sys.argv) >= 3:
        recovery.run(date_from=sys.argv[1], date_to=sys.argv[2])
    elif len(sys.argv) == 2:
        recovery.run(date_from=sys.argv[1], date_to=sys.argv[1])
    else:
        recovery.run()


if __name__ == '__main__':
    main()
