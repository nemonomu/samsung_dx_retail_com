"""Best Buy 리뷰 네트워크 요청 진단 스크립트 v3
- 리뷰 탭 실제 클릭하여 GraphQL 요청 트리거
- request postData (operationName) 캡처
- 리뷰 GraphQL operation 식별
"""
import time
import re
import json
import psycopg2
from DrissionPage import ChromiumPage, ChromiumOptions
from config import DB_CONFIG


def get_test_url():
    """DB에서 리뷰 있는 제품 URL 1개 가져오기"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT product_url, retailer_sku_name, count_of_reviews
            FROM bby_tv_crawl
            WHERE account_name = 'Bestbuy'
              AND CAST(NULLIF(REPLACE(count_of_reviews, ',', ''), '') AS INTEGER) > 100
              AND detailed_review_content IS NULL
            ORDER BY crawl_datetime DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row:
            return row[0], row[1], row[2]
        return None, None, None
    except Exception as e:
        print(f"[ERROR] DB: {e}")
        return None, None, None


def analyze_packet(packet, label=""):
    """패킷 상세 분석"""
    url = packet.url if packet.url else ''
    print(f"    [{label}] {url[:100]}")

    # request body 읽기 (여러 방식 시도)
    req_body = None
    for attr in ['body', 'postData', 'data']:
        try:
            val = getattr(packet.request, attr, None)
            if val:
                req_body = val
                break
        except:
            continue

    if req_body:
        try:
            if isinstance(req_body, str):
                req_data = json.loads(req_body)
            else:
                req_data = req_body

            if isinstance(req_data, dict):
                op = req_data.get('operationName', 'N/A')
                print(f"      operationName: {op}")
                variables = req_data.get('variables', {})
                print(f"      variables: {json.dumps(variables, ensure_ascii=False)[:300]}")
            elif isinstance(req_data, list):
                for j, item in enumerate(req_data[:3]):
                    if isinstance(item, dict):
                        print(f"      [{j}] op: {item.get('operationName', 'N/A')}")
                        variables = item.get('variables', {})
                        print(f"      [{j}] vars: {json.dumps(variables, ensure_ascii=False)[:200]}")
        except Exception as e:
            print(f"      req_body type: {type(req_body).__name__}, preview: {str(req_body)[:200]}")

    # response body
    try:
        resp_body = packet.response.body
        if isinstance(resp_body, dict):
            print(f"      resp keys: {list(resp_body.keys())[:10]}")
            if 'data' in resp_body and isinstance(resp_body['data'], dict):
                data_keys = list(resp_body['data'].keys())
                print(f"      data keys: {data_keys}")
                # review 관련 키 상세 출력
                for key in data_keys:
                    val = resp_body['data'][key]
                    if isinstance(val, dict):
                        print(f"        {key} keys: {list(val.keys())[:15]}")
                    elif isinstance(val, list):
                        print(f"        {key}: list[{len(val)}]")
                        if val and isinstance(val[0], dict):
                            print(f"          first item keys: {list(val[0].keys())[:10]}")
        elif isinstance(resp_body, list):
            print(f"      resp: list[{len(resp_body)}]")
    except Exception as e:
        print(f"      [resp error] {e}")


def main():
    print("=" * 80)
    print("  Best Buy Review Network Diagnostic v3")
    print("=" * 80)

    test_url, sku_name, review_count = get_test_url()
    if not test_url:
        print("[ERROR] No test URL found")
        return

    print(f"\n[TEST] {sku_name[:60]}...")
    print(f"  URL: {test_url}")
    print(f"  Reviews: {review_count}")

    co = ChromiumOptions()
    page = ChromiumPage(co)

    # 1. 제품 페이지 접근
    print(f"\n[1] Loading product page...")
    page.get(test_url)
    time.sleep(5)
    print(f"    Title: {page.title[:80]}")

    # 2. Customer Reviews 탭 찾기
    print(f"\n[2] Finding Customer Reviews tab...")

    # JS로 리뷰 탭 링크/버튼 찾기
    tab_info = page.run_js('''
        var result = [];
        // 모든 링크/버튼에서 "review" 텍스트 검색
        var allElements = document.querySelectorAll('a, button, [role="tab"]');
        for (var i = 0; i < allElements.length; i++) {
            var el = allElements[i];
            var text = el.textContent.trim().toLowerCase();
            if (text.includes('review') && text.length < 100) {
                result.push({
                    tag: el.tagName,
                    text: el.textContent.trim().substring(0, 80),
                    id: el.id || '',
                    className: el.className || '',
                    href: el.href || '',
                    role: el.getAttribute('role') || ''
                });
            }
        }
        return result;
    ''')

    if tab_info:
        print(f"    Found {len(tab_info)} review-related elements:")
        for i, info in enumerate(tab_info):
            print(f"      [{i}] <{info['tag']}> text=\"{info['text'][:60]}\"")
            print(f"           class=\"{info['className'][:80]}\" id=\"{info['id']}\" role=\"{info['role']}\"")
    else:
        print("    No review-related elements found")

    # 3. 네트워크 리스닝 시작 후 리뷰 탭 클릭
    print(f"\n[3] Starting listener, then clicking reviews tab...")
    page.listen.start('graphql')

    # 리뷰 섹션으로 스크롤 + 탭 클릭
    click_result = page.run_js('''
        // 리뷰 탭 찾기 및 클릭
        var tabs = document.querySelectorAll('a, button, [role="tab"]');
        for (var i = 0; i < tabs.length; i++) {
            var text = tabs[i].textContent.trim().toLowerCase();
            if ((text.includes('customer review') || text.includes('reviews'))
                && !text.includes('write') && text.length < 50) {
                tabs[i].scrollIntoView({behavior: "smooth", block: "center"});
                tabs[i].click();
                return 'clicked: ' + tabs[i].textContent.trim().substring(0, 50);
            }
        }
        // fallback: #tabbed-customerreviews 앵커로 스크롤
        var anchor = document.getElementById('tabbed-customerreviews');
        if (anchor) {
            anchor.scrollIntoView({behavior: "smooth", block: "start"});
            return 'scrolled to #tabbed-customerreviews';
        }
        return 'not found';
    ''')
    print(f"    Result: {click_result}")
    time.sleep(5)

    # 4. GraphQL 패킷 수집
    print(f"\n[4] Capturing GraphQL requests after tab click...")
    captured = 0
    for _ in range(20):
        packet = page.listen.wait(timeout=2)
        if not packet:
            break
        captured += 1
        analyze_packet(packet, f"GQL-{captured}")

    page.listen.stop()

    if captured == 0:
        print("    No GraphQL requests after tab click")

        # Fallback: See All Reviews 버튼 클릭 시도
        print(f"\n[5] Trying 'See All' button...")
        page.listen.start('graphql')

        btn_result = page.run_js('''
            var btns = document.querySelectorAll('button, a');
            for (var i = 0; i < btns.length; i++) {
                var text = btns[i].textContent.trim().toLowerCase();
                if (text.includes('see all') && text.includes('review')) {
                    btns[i].scrollIntoView({behavior: "smooth", block: "center"});
                    btns[i].click();
                    return 'clicked: ' + btns[i].textContent.trim().substring(0, 50);
                }
            }
            return 'not found';
        ''')
        print(f"    Result: {btn_result}")
        time.sleep(5)

        for _ in range(20):
            packet = page.listen.wait(timeout=2)
            if not packet:
                break
            analyze_packet(packet, "GQL-SeeAll")

        page.listen.stop()

    # 6. 현재 DOM에서 리뷰 콘텐츠 확인
    print(f"\n[6] Checking DOM for review content...")
    dom_check = page.run_js('''
        var result = {};
        result.preWhiteSpace = document.querySelectorAll('p.pre-white-space').length;
        result.reviewItem = document.querySelectorAll('li.review-item').length;
        result.ugcReviewBody = document.querySelectorAll('.ugc-review-body').length;
        result.reviewText = document.querySelectorAll('[class*="review"]').length;

        // 리뷰 텍스트 샘플
        var firstReview = document.querySelector('p.pre-white-space');
        result.firstReviewText = firstReview ? firstReview.textContent.trim().substring(0, 100) : null;

        // 현재 URL
        result.currentUrl = window.location.href;

        return result;
    ''')
    if dom_check:
        print(f"    p.pre-white-space: {dom_check.get('preWhiteSpace', 0)}")
        print(f"    li.review-item: {dom_check.get('reviewItem', 0)}")
        print(f"    .ugc-review-body: {dom_check.get('ugcReviewBody', 0)}")
        print(f"    [class*='review']: {dom_check.get('reviewText', 0)}")
        print(f"    First review: {dom_check.get('firstReviewText', 'None')}")
        print(f"    Current URL: {dom_check.get('currentUrl', '')[:100]}")

    page.quit()
    print("\nDone.")


if __name__ == '__main__':
    main()
