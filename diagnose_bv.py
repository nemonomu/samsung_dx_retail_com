"""Best Buy 리뷰 페이지 네트워크 요청 진단 스크립트
리뷰 탭 이동 시 어떤 API가 호출되는지 확인
"""
import time
import re
from DrissionPage import ChromiumPage, ChromiumOptions

# 테스트 URL (리뷰 있는 제품)
TEST_URL = "https://www.bestbuy.com/product/insignia-50-class-f50-series-led-4k-uhd-smart-fire-tv/JJ8RL4J6CX"

co = ChromiumOptions()
page = ChromiumPage(co)

print("=" * 80)
print("  Best Buy Review Network Request Diagnostic")
print("=" * 80)

# 1. 제품 페이지 접근
print(f"\n[1] Loading product page: {TEST_URL[:80]}...")
page.get(TEST_URL)
time.sleep(3)

# SKU 추출
page_html = page.html
match = re.search(r'mbo-entrypoint-(\d+)', page_html)
sku = match.group(1) if match else 'unknown'
print(f"    SKU: {sku}")

# 2. 모든 네트워크 요청 리스닝 시작 (필터 없음)
print(f"\n[2] Starting network listener (no filter)...")
page.listen.start()

# 3. 리뷰 섹션으로 스크롤
print(f"\n[3] Scrolling to trigger review loading...")
for scroll_pct in [0.3, 0.5, 0.7, 0.85, 1.0]:
    page.run_js(f"window.scrollTo(0, document.body.scrollHeight * {scroll_pct})")
    time.sleep(1.5)
    print(f"    Scrolled to {int(scroll_pct*100)}%")

time.sleep(3)

# 4. 캡처된 패킷 수집
print(f"\n[4] Collecting captured packets...")
review_related = []
all_urls = []

for _ in range(50):
    packet = page.listen.wait(timeout=1)
    if not packet:
        break
    url = packet.url if packet.url else ''
    all_urls.append(url)

    # 리뷰 관련 키워드 필터
    keywords = ['review', 'bazaar', 'ugc', 'rating', 'comment', 'feedback', 'mention', 'bv']
    if any(kw in url.lower() for kw in keywords):
        review_related.append(packet)
        print(f"    [REVIEW] {url[:120]}")

page.listen.stop()

print(f"\n[5] Summary:")
print(f"    Total requests captured: {len(all_urls)}")
print(f"    Review-related requests: {len(review_related)}")

if not review_related:
    print(f"\n    No review-related requests found. Showing all captured URLs:")
    for i, url in enumerate(all_urls[:30]):
        print(f"    [{i+1}] {url[:150]}")

# 5. 리뷰 탭 URL로 이동 후 재시도
print(f"\n[6] Navigating to reviews tab URL...")
reviews_url = f"{TEST_URL}/sku/{sku}#tabbed-customerreviews"
print(f"    URL: {reviews_url}")

page.listen.start()
page.get(reviews_url)
time.sleep(5)

# 스크롤
for scroll_pct in [0.3, 0.5, 0.7, 0.85, 1.0]:
    page.run_js(f"window.scrollTo(0, document.body.scrollHeight * {scroll_pct})")
    time.sleep(1)

time.sleep(3)

print(f"\n[7] Packets after reviews tab navigation:")
review_related2 = []
all_urls2 = []

for _ in range(50):
    packet = page.listen.wait(timeout=1)
    if not packet:
        break
    url = packet.url if packet.url else ''
    all_urls2.append(url)

    keywords = ['review', 'bazaar', 'ugc', 'rating', 'comment', 'feedback', 'mention', 'bv']
    if any(kw in url.lower() for kw in keywords):
        review_related2.append(packet)
        print(f"    [REVIEW] {url[:120]}")

page.listen.stop()

print(f"\n    Total requests: {len(all_urls2)}")
print(f"    Review-related: {len(review_related2)}")

if not review_related2:
    print(f"\n    All captured URLs:")
    for i, url in enumerate(all_urls2[:30]):
        print(f"    [{i+1}] {url[:150]}")

# 리뷰 관련 패킷의 응답 확인
for packet in review_related + review_related2:
    try:
        body = packet.response.body
        if isinstance(body, dict):
            print(f"\n    [RESPONSE] {packet.url[:80]}")
            print(f"    Keys: {list(body.keys())[:10]}")
            if 'Results' in body:
                print(f"    Results count: {len(body['Results'])}")
    except:
        pass

page.quit()
print("\nDone.")
