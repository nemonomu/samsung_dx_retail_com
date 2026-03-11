"""Best Buy 리뷰 네트워크 요청 진단 스크립트 v2
- DB에서 실제 product_url 가져와서 테스트
- GraphQL (/gateway/graphql) 요청 캡처
- 리뷰 탭 이동 시 어떤 API가 호출되는지 확인
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
              AND CAST(NULLIF(REPLACE(count_of_reviews, ',', ''), '') AS INTEGER) > 0
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


def main():
    print("=" * 80)
    print("  Best Buy Review Network Diagnostic v2 (GraphQL)")
    print("=" * 80)

    # DB에서 테스트 URL 가져오기
    test_url, sku_name, review_count = get_test_url()
    if not test_url:
        print("[ERROR] No test URL found in DB")
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

    # 현재 URL 확인 (리다이렉트 여부)
    current_url = page.url
    print(f"    Current URL: {current_url[:100]}")

    # SKU 추출 시도
    page_html = page.html
    sku = None
    for pattern in [r'mbo-entrypoint-(\d+)', r'"skuId"\s*:\s*"(\d+)"', r'SKU[:\s]+(\d{7,})']:
        match = re.search(pattern, page_html)
        if match:
            sku = match.group(1)
            print(f"    SKU found ({pattern[:20]}...): {sku}")
            break

    if not sku:
        print("    [WARNING] SKU not found in page source")
        # URL에서 시도
        match = re.search(r'/(\d{7,})', test_url)
        if match:
            sku = match.group(1)
            print(f"    SKU from URL: {sku}")

    # 페이지 타이틀 확인
    title = page.title
    print(f"    Page title: {title[:80]}")

    # 2. 네트워크 리스닝 시작 (graphql + review 관련)
    print(f"\n[2] Starting network listener...")
    page.listen.start()

    # 3. 스크롤하여 리뷰 섹션 트리거
    print(f"\n[3] Scrolling page...")
    for scroll_pct in [0.3, 0.5, 0.7, 0.85, 1.0]:
        page.run_js(f"window.scrollTo(0, document.body.scrollHeight * {scroll_pct})")
        time.sleep(2)
        print(f"    Scrolled to {int(scroll_pct*100)}%")

    time.sleep(3)

    # 4. 패킷 수집 및 분석
    print(f"\n[4] Analyzing captured packets...")
    graphql_packets = []
    review_packets = []
    all_domains = {}

    for _ in range(100):
        packet = page.listen.wait(timeout=1)
        if not packet:
            break
        url = packet.url if packet.url else ''

        # 도메인 카운트
        match = re.search(r'https?://([^/]+)', url)
        if match:
            domain = match.group(1)
            all_domains[domain] = all_domains.get(domain, 0) + 1

        # GraphQL 요청
        if 'graphql' in url.lower():
            graphql_packets.append(packet)
            print(f"    [GRAPHQL] {url[:120]}")

        # 리뷰 관련
        keywords = ['review', 'bazaar', 'ugc', 'rating', 'comment', 'mention', 'bv']
        if any(kw in url.lower() for kw in keywords):
            review_packets.append(packet)
            if 'graphql' not in url.lower():
                print(f"    [REVIEW] {url[:120]}")

    page.listen.stop()

    print(f"\n    Domains accessed:")
    for domain, count in sorted(all_domains.items(), key=lambda x: -x[1])[:15]:
        print(f"      {domain}: {count}")

    # GraphQL 요청 상세 분석
    if graphql_packets:
        print(f"\n[5] GraphQL request details ({len(graphql_packets)} requests):")
        for i, packet in enumerate(graphql_packets[:5]):
            print(f"\n    --- GraphQL #{i+1} ---")
            try:
                # 요청 body 확인
                req_body = packet.request.body
                if req_body:
                    if isinstance(req_body, str):
                        try:
                            req_data = json.loads(req_body)
                        except:
                            req_data = req_body
                    else:
                        req_data = req_body

                    if isinstance(req_data, dict):
                        op_name = req_data.get('operationName', 'N/A')
                        print(f"    operationName: {op_name}")
                        variables = req_data.get('variables', {})
                        print(f"    variables: {json.dumps(variables, ensure_ascii=False)[:200]}")
                        query = req_data.get('query', '')
                        if query:
                            print(f"    query (first 200): {query[:200]}")
                    elif isinstance(req_data, list):
                        for j, item in enumerate(req_data[:3]):
                            if isinstance(item, dict):
                                op_name = item.get('operationName', 'N/A')
                                print(f"    [{j}] operationName: {op_name}")
                    else:
                        print(f"    body: {str(req_data)[:200]}")
            except Exception as e:
                print(f"    [ERROR reading request] {e}")

            try:
                # 응답 body 확인
                resp_body = packet.response.body
                if isinstance(resp_body, dict):
                    print(f"    response keys: {list(resp_body.keys())[:10]}")
                    if 'data' in resp_body:
                        data_keys = list(resp_body['data'].keys()) if isinstance(resp_body['data'], dict) else type(resp_body['data'])
                        print(f"    data keys: {data_keys}")
                elif isinstance(resp_body, list):
                    print(f"    response: list of {len(resp_body)} items")
                    if resp_body and isinstance(resp_body[0], dict):
                        print(f"    first item keys: {list(resp_body[0].keys())[:10]}")
            except Exception as e:
                print(f"    [ERROR reading response] {e}")
    else:
        print(f"\n[5] No GraphQL requests captured during scroll")

    # 6. 리뷰 탭 클릭 시도
    if sku:
        print(f"\n[6] Trying reviews tab navigation...")
        reviews_url = f"{test_url}/sku/{sku}#tabbed-customerreviews"
        print(f"    URL: {reviews_url}")

        page.listen.start()
        page.get(reviews_url)
        time.sleep(5)

        # 스크롤
        for scroll_pct in [0.3, 0.5, 0.7, 1.0]:
            page.run_js(f"window.scrollTo(0, document.body.scrollHeight * {scroll_pct})")
            time.sleep(1.5)

        time.sleep(3)

        print(f"\n[7] Packets after reviews tab:")
        for _ in range(100):
            packet = page.listen.wait(timeout=1)
            if not packet:
                break
            url = packet.url if packet.url else ''

            if 'graphql' in url.lower():
                print(f"    [GRAPHQL] {url[:80]}")
                try:
                    req_body = packet.request.body
                    if isinstance(req_body, str):
                        req_data = json.loads(req_body)
                    else:
                        req_data = req_body
                    if isinstance(req_data, dict):
                        print(f"      op: {req_data.get('operationName', 'N/A')}")
                    elif isinstance(req_data, list):
                        for item in req_data[:3]:
                            if isinstance(item, dict):
                                print(f"      op: {item.get('operationName', 'N/A')}")
                except:
                    pass

            keywords = ['review', 'bazaar', 'ugc', 'rating', 'mention', 'bv']
            if any(kw in url.lower() for kw in keywords) and 'graphql' not in url.lower():
                print(f"    [REVIEW] {url[:120]}")

        page.listen.stop()

    page.quit()
    print("\nDone.")


if __name__ == '__main__':
    main()
