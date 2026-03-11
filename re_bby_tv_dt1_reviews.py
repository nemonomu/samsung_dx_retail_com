"""
bby_tv_dt1 Review Recovery Script
- detailed_review_content, top_mentions, summarized_review_content NULL 복구
- count_of_reviews > 0인데 위 컬럼이 NULL인 레코드 대상
- 리뷰 페이지 접근 → 추출 → bby_tv_crawl + tv_retail_com UPDATE

사용법:
    python re_bby_tv_dt1_reviews.py                          # 날짜 입력 모드
    python re_bby_tv_dt1_reviews.py 2026-03-11               # 특정 날짜
    python re_bby_tv_dt1_reviews.py 2026-03-10 2026-03-11    # 날짜 범위
"""
import time
import re
import sys
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

    def navigate_to_reviews_page(self, product_url):
        """리뷰 페이지로 이동 - 제품 페이지 → 스크롤 → See All Customer Reviews 버튼 클릭
        /site/reviews/ URL은 제품 페이지로 리다이렉트되므로 버튼 클릭 방식 사용
        """
        try:
            # 1. 제품 페이지 접근
            self.page.get(product_url)

            # h1 로딩 대기
            h1_elem = self.page.ele(
                'xpath://h1[contains(@class, "h4") or contains(@class, "heading")]',
                timeout=20
            )
            if not h1_elem:
                print(f"    [ERROR] Product page load failed")
                return False

            # 2. See All Customer Reviews 버튼 검색 (단계적 스크롤)
            selectors = self.config.get_xpath_list('see_all_reviews_btn', self.file_name) or [
                'xpath://button[contains(., "See All Customer Reviews")]',
            ]

            # DOM에서 바로 검색
            for selector in selectors:
                try:
                    button = self.page.ele(selector, timeout=2)
                    if button:
                        print("    [OK] See All Reviews button found (in DOM)")
                        button.scroll.to_see()
                        time.sleep(0.5)
                        button.click()
                        time.sleep(5)

                        # 버튼 클릭 후 URL 확인
                        current_url = self.page.url
                        print(f"    [INFO] After click URL: {current_url[:100]}")
                        return True
                except:
                    continue

            # 단계적 스크롤 후 검색
            for scroll_pct in [0.5, 0.7, 0.85, 1.0]:
                self.page.run_js(f"window.scrollTo(0, document.body.scrollHeight * {scroll_pct})")
                time.sleep(1.5)
                for selector in selectors:
                    try:
                        button = self.page.ele(selector, timeout=2)
                        if button:
                            print(f"    [OK] See All Reviews button found (after scroll {int(scroll_pct*100)}%)")
                            button.scroll.to_see()
                            time.sleep(0.5)
                            button.click()
                            time.sleep(5)

                            current_url = self.page.url
                            print(f"    [INFO] After click URL: {current_url[:100]}")
                            return True
                    except:
                        continue

            print("    [WARNING] See All Reviews button not found")
            return False

        except Exception as e:
            print(f"    [ERROR] Navigate to reviews page failed: {e}")
            return False

    def extract_all_review_data(self):
        """리뷰 페이지에서 모든 리뷰 데이터 추출 - DrissionPage eles() 사용 (live DOM 직접 접근)"""
        result = {
            'detailed_review_content': None,
            'top_mentions': None,
            'summarized_review_content': None,
            'recommendation_intent': None
        }

        # 리뷰 콘텐츠 DOM 렌더링 대기
        try:
            self.page.ele('xpath://li[@class="review-item"]//p[@class="pre-white-space"]', timeout=10)
            print("    [OK] Review content rendered")
        except:
            print("    [WARNING] Review content DOM wait timeout, waiting 3s fallback")
            time.sleep(3)

        # 1. Top Mentions (eles로 직접 추출)
        result['top_mentions'] = self.extract_top_mentions()
        print(f"    [✓] Top_Mentions: {result['top_mentions'][:50] if result['top_mentions'] else 'None'}")

        # 2. Recommendation Intent
        result['recommendation_intent'] = self.extract_recommendation_intent()
        print(f"    [✓] Recommendation_Intent: {result['recommendation_intent']}")

        # 3. Detailed Reviews (페이지네이션 포함)
        result['detailed_review_content'] = self.extract_reviews()
        review_len = len(result['detailed_review_content']) if result['detailed_review_content'] else 0
        print(f"    [✓] Detailed_Reviews: {review_len} chars")

        # 4. Summarized Review Content
        result['summarized_review_content'] = self.extract_summarized_review_content()
        print(f"    [✓] Summarized_Review: {result['summarized_review_content'][:50] if result['summarized_review_content'] else 'None'}")

        return result

    def extract_top_mentions(self):
        """Top Mentions 추출 - DrissionPage eles() 사용"""
        try:
            mentions = []
            # 1순위: Pros + Cons data-feature-name
            try:
                pros = self.page.eles('xpath://div[contains(@class, "pros-container")]//button[@data-feature-name]')
                for btn in pros:
                    name = btn.attr('data-feature-name')
                    if name:
                        mentions.append(name.strip())
                cons = self.page.eles('xpath://div[contains(@class, "cons-container")]//button[@data-feature-name]')
                for btn in cons:
                    name = btn.attr('data-feature-name')
                    if name:
                        mentions.append(name.strip())
            except:
                pass

            if mentions:
                return ', '.join(mentions)

            # 2순위: 기존 패턴
            xpaths = [
                'xpath://ul[@class="list-unstyled"]/li/a',
                'xpath://div[contains(@class, "customer-review-pros-stats")]//span[@class="text-nowrap"]',
            ]
            for xpath in xpaths:
                try:
                    elems = self.page.eles(xpath)
                    if elems:
                        for elem in elems:
                            text = elem.text.strip() if elem.text else ''
                            if text:
                                clean_text = re.sub(r'\s*\([\d,]+\)\s*$', '', text)
                                clean_text = clean_text.replace('\xa0', ' ').strip()
                                if clean_text:
                                    mentions.append(clean_text)
                        break
                except:
                    continue
            return ', '.join(mentions) if mentions else None
        except Exception as e:
            print(f"    [ERROR] Top mentions extraction failed: {e}")
            return None

    def extract_recommendation_intent(self):
        """Recommendation Intent 추출 - DrissionPage ele() 사용"""
        try:
            xpaths = [
                'xpath://span[contains(@class, "recommendation-percent")]',
            ]
            for xpath in xpaths:
                try:
                    elem = self.page.ele(xpath, timeout=2)
                    if elem:
                        percent = elem.text.strip() if elem.text else ''
                        if percent:
                            return f"{percent} would recommend to a friend"
                except:
                    continue
            return None
        except Exception as e:
            print(f"    [ERROR] Recommendation intent extraction failed: {e}")
            return None

    def extract_reviews(self):
        """리뷰 20개 추출 (페이지네이션 포함) - DrissionPage eles() 사용"""
        try:
            reviews = []
            collected = 0
            page_num = 1

            while collected < 20:
                # live DOM에서 직접 리뷰 요소 검색
                review_elems = self.page.eles('xpath://li[@class="review-item"]//p[@class="pre-white-space"]')

                if not review_elems:
                    # fallback
                    review_elems = self.page.eles('xpath://p[@class="pre-white-space"]')

                for elem in review_elems:
                    if collected >= 20:
                        break
                    review_text = elem.text.strip() if elem.text else ''
                    if not review_text:
                        # text가 비어있으면 inner_text 시도
                        try:
                            review_text = ' '.join(elem.raw_text.split()) if elem.raw_text else ''
                        except:
                            pass
                    if review_text:
                        collected += 1
                        reviews.append(f"review{collected} - {review_text}")

                if collected >= 20:
                    break

                # next page
                try:
                    next_button = self.page.ele('xpath://li[contains(@class, "page next")]//a', timeout=3)
                    if next_button:
                        print(f"    [INFO] Next page... (Page {page_num + 1})")
                        next_button.scroll.to_see()
                        time.sleep(2)
                        next_button.click()
                        time.sleep(4)
                        # 다음 페이지 리뷰 렌더링 대기
                        try:
                            self.page.ele('xpath://li[@class="review-item"]//p[@class="pre-white-space"]', timeout=10)
                        except:
                            time.sleep(3)
                        page_num += 1
                    else:
                        break
                except:
                    break

            return ' ||| '.join(reviews) if reviews else None
        except Exception as e:
            print(f"    [ERROR] Review extraction failed: {e}")
            return None

    def extract_summarized_review_content(self):
        """Summarized Review Content 추출 - DrissionPage ele() 사용"""
        try:
            xpaths = [
                'xpath://p[@class="mb-200 mt-none"]',
                'xpath://div[@id="reviews-accordion"]//p[contains(@class, "mb-200")]',
            ]
            for xpath in xpaths:
                try:
                    elem = self.page.ele(xpath, timeout=2)
                    if elem:
                        text = elem.text.strip() if elem.text else ''
                        if text:
                            return text
                except:
                    continue
            return None
        except Exception as e:
            print(f"    [ERROR] Summarized review extraction failed: {e}")
            return None

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
        print("  bby_tv_dt1 Review Recovery Script")
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
                original_dt = rec['crawl_datetime']
                print(f"\n[{i+1}/{len(null_records)}] {rec['retailer_sku_name'][:60]}...")
                print(f"  URL: {url[:80]}...")

                # 리뷰 페이지 접근
                if not self.navigate_to_reviews_page(url):
                    print(f"    [FAIL] Could not access reviews page")
                    fail_count += 1
                    time.sleep(2)
                    continue

                # 리뷰 데이터 추출
                data = self.extract_all_review_data()

                if not data['detailed_review_content']:
                    print(f"    [FAIL] detailed_review_content still NULL")
                    fail_count += 1
                    time.sleep(2)
                    continue

                # DB UPDATE (같은 URL의 해당 날짜 범위 레코드 모두)
                cursor = self.db_conn.cursor()
                detail_table = self.config.get_table('detail_data') or 'bby_tv_crawl'

                # 같은 URL + 같은 날짜 범위의 모든 NULL 레코드 업데이트
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

                time.sleep(3)

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
