"""
bby_tv_dt1 Review Recovery Script
- detailed_review_content, top_mentions, summarized_review_content NULL 복구
- count_of_reviews > 0인데 위 컬럼이 NULL인 레코드 대상
- GraphQL 네트워크 캡처로 리뷰 데이터 직접 획득 → bby_tv_crawl + tv_retail_com UPDATE

사용법:
    python re_bby_tv_dt1_reviews.py                          # 날짜 입력 모드
    python re_bby_tv_dt1_reviews.py 2026-03-11               # 특정 날짜
    python re_bby_tv_dt1_reviews.py 2026-03-10 2026-03-11    # 날짜 범위
"""
import time
import re
import sys
import json
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

    def extract_numeric_sku(self, product_url):
        """페이지 소스에서 numeric SKU 추출"""
        try:
            page_html = self.page.html
            for pattern in [r'mbo-entrypoint-(\d+)', r'"skuId"\s*:\s*"(\d+)"', r'SKU[:\s]+(\d{7,})']:
                match = re.search(pattern, page_html)
                if match:
                    return match.group(1)
        except:
            pass
        return None

    def click_reviews_and_capture_graphql(self, numeric_sku):
        """제품 페이지에서 리뷰 링크 클릭 → GraphQL 응답 캡처

        캡처 대상 operations:
        - CustomerReviewList_Init → reviews (리뷰 본문)
        - Reviews_Pros_Cons_Init → reviewInfo (top mentions)
        - Ai_Review_Summary_Init → reviewInfo (AI 요약)
        """
        captured_data = {
            'reviews': None,           # CustomerReviewList_Init
            'pros_cons': None,         # Reviews_Pros_Cons_Init
            'ai_summary': None,        # Ai_Review_Summary_Init
            'rating_card': None,       # CustomerRatingCard_Init
        }

        try:
            # 1. GraphQL 리스닝 시작
            self.page.listen.start('graphql')

            # 2. 제품 자체의 리뷰 링크 클릭 (추천 제품이 아닌 메인 제품의 리뷰)
            click_result = self.page.run_js(f'''
                // 메인 제품의 리뷰 링크 찾기 (SKU 기반으로 정확한 것 클릭)
                // 방법 1: 페이지 상단의 star rating 링크 (가장 정확)
                var ratingLink = document.querySelector(
                    '.price-ratings a[href*="customerreviews"], '
                    + '.c-stars-reviews a, '
                    + 'a[href*="tabbed-customerreviews"]'
                );
                if (ratingLink) {{
                    ratingLink.scrollIntoView({{behavior: "smooth", block: "center"}});
                    ratingLink.click();
                    return 'clicked rating link: ' + ratingLink.textContent.trim().substring(0, 50);
                }}

                // 방법 2: 첫 번째 review 링크 (보통 메인 제품)
                var links = document.querySelectorAll('a');
                for (var i = 0; i < links.length; i++) {{
                    var text = links[i].textContent.trim().toLowerCase();
                    var href = links[i].href || '';
                    if (text.includes('review') && href.includes('customerreviews')
                        && !text.includes('write')) {{
                        links[i].scrollIntoView({{behavior: "smooth", block: "center"}});
                        links[i].click();
                        return 'clicked: ' + links[i].textContent.trim().substring(0, 50);
                    }}
                }}

                // 방법 3: See All Customer Reviews 버튼
                var btns = document.querySelectorAll('button, a');
                for (var i = 0; i < btns.length; i++) {{
                    var text = btns[i].textContent.trim().toLowerCase();
                    if (text.includes('see all') && text.includes('review')) {{
                        btns[i].scrollIntoView({{behavior: "smooth", block: "center"}});
                        btns[i].click();
                        return 'clicked see all: ' + btns[i].textContent.trim().substring(0, 50);
                    }}
                }}

                return 'not found';
            ''')
            print(f"    [INFO] Click result: {click_result}")

            if click_result == 'not found':
                self.page.listen.stop()
                return captured_data

            # 3. GraphQL 응답 수집 (최대 15초)
            time.sleep(3)
            target_ops = {
                'CustomerReviewList_Init': 'reviews',
                'Reviews_Pros_Cons_Init': 'pros_cons',
                'Ai_Review_Summary_Init': 'ai_summary',
                'CustomerRatingCard_Init': 'rating_card',
            }

            for _ in range(30):
                packet = self.page.listen.wait(timeout=1)
                if not packet:
                    # 모든 타겟 획득했으면 조기 종료
                    if captured_data['reviews']:
                        break
                    continue

                # operationName 확인
                try:
                    req_body = None
                    for attr in ['body', 'postData', 'data']:
                        val = getattr(packet.request, attr, None)
                        if val:
                            req_body = val
                            break

                    if req_body:
                        if isinstance(req_body, str):
                            req_data = json.loads(req_body)
                        else:
                            req_data = req_body

                        op_name = None
                        if isinstance(req_data, dict):
                            op_name = req_data.get('operationName')
                        elif isinstance(req_data, list) and req_data:
                            op_name = req_data[0].get('operationName') if isinstance(req_data[0], dict) else None

                        if op_name and op_name in target_ops:
                            key = target_ops[op_name]
                            try:
                                resp_body = packet.response.body
                                if resp_body:
                                    captured_data[key] = resp_body
                                    print(f"    [OK] Captured: {op_name}")
                            except:
                                pass
                except:
                    continue

            self.page.listen.stop()

        except Exception as e:
            print(f"    [ERROR] GraphQL capture failed: {e}")
            try:
                self.page.listen.stop()
            except:
                pass

        return captured_data

    def parse_graphql_reviews(self, captured_data):
        """GraphQL 캡처 데이터에서 리뷰 정보 파싱"""
        result = {
            'detailed_review_content': None,
            'top_mentions': None,
            'summarized_review_content': None,
            'recommendation_intent': None
        }

        # 1. Detailed Reviews (CustomerReviewList_Init)
        reviews_data = captured_data.get('reviews')
        if reviews_data:
            try:
                product = reviews_data.get('data', {}).get('productBySkuId', {})
                reviews_list = product.get('reviews', {})

                # reviews가 dict일 수 있음 (edges/nodes 패턴 또는 직접 리스트)
                review_items = []
                if isinstance(reviews_list, dict):
                    # edges 패턴
                    edges = reviews_list.get('edges', [])
                    if edges:
                        review_items = [e.get('node', e) for e in edges]
                    # customerReviews 키
                    elif 'customerReviews' in reviews_list:
                        review_items = reviews_list['customerReviews']
                    # topics/items
                    elif 'topics' in reviews_list:
                        for topic in reviews_list.get('topics', []):
                            if isinstance(topic, dict):
                                items = topic.get('reviews', [])
                                review_items.extend(items)
                    else:
                        # 직접 리스트 값 찾기
                        for key, val in reviews_list.items():
                            if isinstance(val, list) and val and isinstance(val[0], dict):
                                review_items = val
                                break
                elif isinstance(reviews_list, list):
                    review_items = reviews_list

                if review_items:
                    formatted = []
                    for i, review in enumerate(review_items[:20], 1):
                        if isinstance(review, dict):
                            # 다양한 키 이름으로 리뷰 텍스트 찾기
                            text = (review.get('reviewText') or review.get('text')
                                    or review.get('comment') or review.get('body')
                                    or review.get('content', '')).strip()
                            if text:
                                formatted.append(f"review{i} - {text}")
                    if formatted:
                        result['detailed_review_content'] = ' ||| '.join(formatted)
                        print(f"    [OK] Reviews: {len(formatted)} extracted, {len(result['detailed_review_content'])} chars")
                    else:
                        # 구조 디버그
                        if review_items:
                            sample = review_items[0] if isinstance(review_items[0], dict) else review_items[0]
                            print(f"    [DEBUG] review item keys: {list(sample.keys()) if isinstance(sample, dict) else type(sample)}")
                        print(f"    [WARNING] Review items found but no text extracted")
                else:
                    print(f"    [DEBUG] reviews structure: {type(reviews_list)}, keys: {list(reviews_list.keys()) if isinstance(reviews_list, dict) else 'N/A'}")
            except Exception as e:
                print(f"    [ERROR] Review parsing failed: {e}")

        # 2. Top Mentions (Reviews_Pros_Cons_Init)
        pros_cons_data = captured_data.get('pros_cons')
        if pros_cons_data:
            try:
                product = pros_cons_data.get('data', {}).get('productBySkuId', {})
                review_info = product.get('reviewInfo', {})

                mentions = []
                # proFeatures / conFeatures (Best Buy GraphQL 키)
                for features_key in ['proFeatures', 'pros', 'conFeatures', 'cons']:
                    features = review_info.get(features_key, [])
                    if isinstance(features, list):
                        for f in features:
                            if isinstance(f, dict):
                                name = (f.get('name') or f.get('label') or f.get('text')
                                        or f.get('feature') or f.get('featureName', ''))
                            else:
                                name = str(f)
                            if name:
                                mentions.append(name)

                if mentions:
                    result['top_mentions'] = ', '.join(mentions)
                    print(f"    [OK] Top Mentions: {result['top_mentions'][:80]}")
                else:
                    print(f"    [DEBUG] reviewInfo keys: {list(review_info.keys()) if isinstance(review_info, dict) else 'N/A'}")
            except Exception as e:
                print(f"    [ERROR] Pros/Cons parsing failed: {e}")

        # 3. AI Summary (Ai_Review_Summary_Init)
        ai_data = captured_data.get('ai_summary')
        if ai_data:
            try:
                product = ai_data.get('data', {}).get('productBySkuId', {})
                review_info = product.get('reviewInfo', {})

                # AI 요약 텍스트 찾기
                summary = (review_info.get('aiSummary') or review_info.get('summary')
                          or review_info.get('reviewSummary'))
                if isinstance(summary, dict):
                    summary = summary.get('text') or summary.get('content') or summary.get('summary', '')
                if summary:
                    result['summarized_review_content'] = str(summary).strip()
                    print(f"    [OK] AI Summary: {result['summarized_review_content'][:60]}...")
                else:
                    print(f"    [DEBUG] ai reviewInfo keys: {list(review_info.keys()) if isinstance(review_info, dict) else 'N/A'}")
            except Exception as e:
                print(f"    [ERROR] AI Summary parsing failed: {e}")

        # 4. Recommendation Intent (CustomerRatingCard_Init)
        rating_data = captured_data.get('rating_card')
        if rating_data:
            try:
                product = rating_data.get('data', {}).get('productBySkuId', {})
                review_info = product.get('reviewInfo', {})

                rec = (review_info.get('recommendedPercent')
                       or review_info.get('recommendedPercentage')
                       or review_info.get('recommendPercent'))
                if rec is not None:
                    result['recommendation_intent'] = f"{rec}% would recommend to a friend"
                    print(f"    [OK] Recommendation: {result['recommendation_intent']}")
                else:
                    print(f"    [DEBUG] rating reviewInfo keys: {list(review_info.keys()) if isinstance(review_info, dict) else 'N/A'}")
            except Exception as e:
                print(f"    [ERROR] Recommendation parsing failed: {e}")

        return result

    def extract_reviews_from_dom(self):
        """리뷰 탭 클릭 후 DOM에서 리뷰 20개 수집 (페이지네이션 포함)
        저장 형식: "review1 - 내용 ||| review2 - 내용 ||| ... ||| review20 - 내용"
        """
        reviews = []
        collected = 0
        page_num = 1

        try:
            # 리뷰 콘텐츠 렌더링 대기
            time.sleep(2)

            while collected < 20:
                # JS로 현재 페이지의 리뷰 텍스트 추출
                page_reviews = self.page.run_js('''
                    var reviews = [];
                    document.querySelectorAll('p.pre-white-space').forEach(function(el) {
                        var text = el.textContent.trim();
                        if (text && text.length > 10) reviews.push(text);
                    });
                    return reviews;
                ''')

                if page_reviews:
                    for text in page_reviews:
                        if collected >= 20:
                            break
                        collected += 1
                        reviews.append(f"review{collected} - {text}")
                    print(f"    [INFO] Page {page_num}: {len(page_reviews)} reviews (total: {collected}/20)")

                if collected >= 20:
                    break

                # 다음 페이지 버튼 클릭
                has_next = self.page.run_js('''
                    var nextBtn = document.querySelector('li.page.next a, a[aria-label="Next"]');
                    if (nextBtn) {
                        nextBtn.scrollIntoView({behavior: "smooth", block: "center"});
                        nextBtn.click();
                        return true;
                    }
                    return false;
                ''')

                if not has_next:
                    break

                page_num += 1
                time.sleep(4)

        except Exception as e:
            print(f"    [ERROR] DOM review extraction failed: {e}")

        if reviews:
            result = ' ||| '.join(reviews)
            print(f"    [OK] Detailed_Reviews: {collected} reviews, {len(result)} chars")
            return result

        print(f"    [WARNING] No reviews found in DOM")
        return None

    def update_db(self, product_url, crawl_datetime, data):
        """bby_tv_crawl + tv_retail_com UPDATE"""
        try:
            cursor = self.db_conn.cursor()
            updated = 0

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
        print("  bby_tv_dt1 Review Recovery Script (GraphQL Capture)")
        print("  - detailed_review_content, top_mentions, summarized_review_content NULL 복구")
        print("=" * 80)

        if not self.connect_db():
            return

        if not date_from:
            date_from = input("Start date (YYYY-MM-DD): ").strip()
        if not date_to:
            date_to = date_from

        print(f"\n[INFO] Recovery range: {date_from} ~ {date_to}")

        summary = self.get_summary(date_from, date_to)
        if not summary:
            print("[ERROR] Summary query failed")
            return

        total, review_null, mentions_null, retail_review_null = summary
        print(f"[INFO] Total records: {total}")
        print(f"[INFO] Detailed_Review NULL (with reviews > 0): {review_null}")
        print(f"[INFO] Top_Mentions NULL (with reviews > 0): {mentions_null}")

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

                # 1. 제품 페이지 접근
                try:
                    self.page.get(url)
                    time.sleep(4)
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

                # 3. 리뷰 링크 클릭 → GraphQL 응답 캡처
                captured_data = self.click_reviews_and_capture_graphql(numeric_sku)

                # 4. 캡처 데이터 파싱
                data = self.parse_graphql_reviews(captured_data)

                # 5. DOM에서 리뷰 20개 수집 (페이지네이션 포함)
                data['detailed_review_content'] = self.extract_reviews_from_dom()

                if not data['detailed_review_content']:
                    print(f"    [FAIL] detailed_review_content still NULL")
                    fail_count += 1
                    time.sleep(2)
                    continue

                # 5. DB UPDATE
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
