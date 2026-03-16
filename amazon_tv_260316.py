import time
import re
import sys
from DrissionPage import ChromiumPage, ChromiumOptions

# Configure stdout encoding for Windows console
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

TEST_URL = "https://www.amazon.com/Samsung-115-Inch-QN90F-Smart-Titan/dp/B09X12GMCH/ref=sr_1_57?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.Qvo7kJfua_Rh0E7tVNvGuHF21xwGp-PkV4qjig2jo6HCZQAoIVwpk0RNpc9VQNz7h5NJtmTu7jn4ofKtgqvs4tSD7St6ezwpqmpQu8UXUGEQm8a2Dq0whVUPxPcueIcpmPd-KlRXBQ0S8R2wV6OsqV_Kgb2TSQEPC8YIQPfzvYBs3y2b_GSdytLP4jmW2lC2QzOi8nKXL3-ML8z7_oeAjErSiJhCxNLeWL3GyPqmYos.WGN_R9iAjcUCa4ZNx6xFRC8Jx4UsKeOB5efNdPEM7OI&dib_tag=se&keywords=TV&qid=1773547289&sprefix=tv%2Caps%2C287&sr=8-57&xpid=Cg0_BApgal-UM"


def extract_detailed_reviews(page):
    """Extract detailed reviews from product detail page (JavaScript 직접 추출)"""
    try:
        # 페이지 하단까지 점진적 스크롤 → lazy loading 트리거
        try:
            for _ in range(5):
                page.run_js("window.scrollBy(0, window.innerHeight)")
                time.sleep(0.5)
            time.sleep(2)
            print(f"  [DEBUG] scrolled to bottom for lazy loading")
        except Exception as e:
            print(f"  [DEBUG] scroll failed: {e}")

        # JavaScript로 리뷰 텍스트 직접 추출
        js_code = """
        var reviews = [];
        var containers = document.querySelectorAll('[id^="customer_review-"], [id^="customer_review_foreign-"]');
        containers.forEach(function(container) {
            var collapsed = container.querySelector('[data-hook="review-collapsed"]');
            if (collapsed) {
                var span = collapsed.querySelector('span');
                if (span && span.innerText.trim().length > 5) {
                    reviews.push(span.innerText.trim());
                }
            }
        });
        return reviews;
        """
        review_texts = page.run_js(js_code)
        print(f"  [DEBUG] JS extracted reviews: {len(review_texts) if review_texts else 0}")

        # JS 실패 시 fallback: DrissionPage XPath
        if not review_texts:
            fallback_xpaths = [
                '//*[starts-with(@id, "customer_review-")]/div[4]/span/div/div[1]/span',
                '//*[starts-with(@id, "customer_review-")]/div[4]/span/div/div[1]',
                '//*[starts-with(@id, "customer_review_foreign-")]/div[4]/span/div/div[1]/span',
                '//*[starts-with(@id, "customer_review_foreign-")]/div[4]/span/div/div[1]',
            ]
            for xpath in fallback_xpaths:
                try:
                    first_elem = page.ele(f'xpath:{xpath}', timeout=10)
                    if first_elem:
                        elems = page.eles(f'xpath:{xpath}')
                        review_texts = []
                        for e in elems:
                            t = ' '.join((e.text or '').split())
                            if t and len(t) > 5:
                                review_texts.append(t)
                        if review_texts:
                            print(f"  [DEBUG] Fallback XPath found: {len(review_texts)} reviews")
                            break
                except Exception:
                    continue

        if not review_texts:
            print(f"  [DEBUG] No review elements found on detail page")
            return None

        all_reviews = []
        collected_reviews = set()
        for text in review_texts:
            review_text = ' '.join(text.split())
            review_text = re.sub(r'\s*Read more\s*$', '', review_text, flags=re.IGNORECASE)
            if review_text and len(review_text) > 5 and review_text not in collected_reviews:
                all_reviews.append(review_text)
                collected_reviews.add(review_text)

        print(f"  [DEBUG] extracted reviews: {len(all_reviews)}")
        if all_reviews:
            formatted_reviews = [f"review{idx} - {review}" for idx, review in enumerate(all_reviews, 1)]
            return ' ||| '.join(formatted_reviews)
        else:
            return None

    except Exception as e:
        print(f"  [WARNING] Failed to extract detailed reviews: {e}")
        return None


if __name__ == '__main__':
    print(f"[TEST] Target URL: {TEST_URL}")
    print(f"[TEST] Starting browser...")

    co = ChromiumOptions()
    co.set_argument('--disable-blink-features=AutomationControlled')
    page = ChromiumPage(co)

    print(f"[TEST] Navigating to product page...")
    page.get(TEST_URL)
    time.sleep(5)

    print(f"[TEST] Page title: {page.title}")
    print(f"[TEST] Extracting detailed reviews...")
    print("=" * 80)

    result = extract_detailed_reviews(page)

    print("=" * 80)
    if result:
        reviews = result.split(' ||| ')
        print(f"[RESULT] Total reviews collected: {len(reviews)}")
        print()
        for review in reviews:
            print(review)
            print("-" * 40)
    else:
        print("[RESULT] No reviews collected (NULL)")

    page.quit()
    print(f"[TEST] Done.")
