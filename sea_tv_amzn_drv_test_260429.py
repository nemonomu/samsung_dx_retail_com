"""
SEA TV Amazon Detail Review Test (260429) — self-contained, no login

amazon_tv_dt1.py의 새 추출 로직(_lazy_load_reviews, extract_detailed_reviews,
extract_summarized_review)과 동일한 코드를 인라인 사본으로 사용.
계정/쿠키 의존을 없애 익명 브라우저로만 테스트.

로그에는 Summarized_Review_Content, Detailed_Review_Content 두 필드만 출력.
"""

import re
import time
import random
import traceback

import psycopg2
from lxml import html
from DrissionPage import ChromiumPage, ChromiumOptions

from config import DB_CONFIG


TEST_URLS = [
    "https://www.amazon.com/Samsung-Electronics-UN32M4500A-32-Inch-Renewed/dp/B07J1WTL8V/ref=sr_1_282?dib=eyJ2IjoiMSJ9.mIfWaZflk8odfpUROMPy3ZeBNrqa_rdE6Dy71BrgnoZ421XS_F0mPh62ireFK4jom3XvUAcojncDvxuZ5eL-Lu1u1Y7JLbhp94lRYcF0R5f5GJoMvWbtuUTH6puqDi5mjJtVwbrnyZ5a0kfeeBN533yQl-3Rp7zX--mSxNPFDZeXEusVzMoiZl8L5GXiDhgGPfjC0-BpV3L15jkY7vUqmcehWHqneSNkudbnl0urzAI.OTWZGZTIwHU29AFtiCjp6clBjifwL6xzW-VwLgVxYk0&dib_tag=se&keywords=tv&qid=1777392372&sr=8-282&xpid=khdVh5qeglJy2&th=1",
    "https://www.amazon.com/TCL-55S425-inch-Smart-Roku/dp/B07JJZPZNS/ref=sr_1_117?dib=eyJ2IjoiMSJ9.hmUv17qrgZ6S9Ng1LSRDOe9jPQLznx2w4z7fK5CCUtYYyJA3JmI3KSczuJbFOK-NUou2VlijROOXs9WUxw3T8-3OUVqTFN_Jm7a5Ae5QLOi7cO9Qc2Mbs2laMiiJTd7D_6JYqhD9gH5rNuC0OBUkSGqYsmiZh9R7DApDwsvToe6ICrCn3krzP4XqrQ2pXIM318cJQ3x2KERRDar5Zxr6eTjrZ3_UWgf04n9uzl2pilE.aBVjgPvJTNptBQLQtCbWpRh4DFjcfFPC2jE4A4m8p5Q&dib_tag=se&keywords=tv&qid=1776960169&sr=8-117&xpid=khdVh5qeglJy2&th=1",
    "https://www.amazon.com/VIZIO-Class-49-5-Diag-Smart/dp/B07KJB92L9/ref=sr_1_291?dib=eyJ2IjoiMSJ9.ZDpEf97BDrt1slTzON_EUt45bn2QxWg6WH-yjv4hPCVmMQzhiZY9XTlbjKp7fbJb8wk_S682-sVsT75E1mX3D0skbpaSPw4zmBfjuxvB-KNb380cBGXKOcmyHcKzFJBK67qM7Gaa-YF6zCW0DA0GZlYwCZqZGMxBBfmFmZ5fcN0gIUD_Q5tWvmdxXlCYIDuZyJdQBYel47dH6QwRObVLY47rkLZabEonTIyAJjF5ZJo.UxBo7Fn5ikjyIlu7EVY_LSVRVBO8QkyfFP745Bq--8A&dib_tag=se&keywords=tv&qid=1776960392&sr=8-291&xpid=khdVh5qeglJy2",
    "https://www.amazon.com/Tyler-TTV705-14-Portable-Battery-Television/dp/B07MC9KHK3/ref=sr_1_259?dib=eyJ2IjoiMSJ9.oAbyxpl5dP_4RJCAQbLQEPF7v2_gyedV-Osr2pvmkz08geN6gOU67muvfCZC483lHLhucwPNH_4GOOC50O3iUis3Lv0iow5s20_kx8J3hO15m6_kAgIvOpgcoEarKjsCDQSy5iO6HCJwqIPiE_T3DgvuIFq4ORNs4rNL_csaBHFGmzuH34KK0Z02sRj041ff0a-KtewWpKczBNuESOmbLJngXnLH4qeekcS4l_SIUhg.X6lhDGAdqyTyuix6gTv_7EKn-Hac4YRbNfBptMxrU0U&dib_tag=se&keywords=tv&qid=1777262764&sbo=RZvfv%2F%2FHxDF%2BO5021pAnSA%3D%3D&sr=8-259&xpid=khdVh5qeglJy2",
]


def load_xpaths_from_db():
    """amazon_tv_config (category='xpath', is_active=TRUE)을 prefix별 priority list로 로드.

    dt1.py:load_xpaths()와 동일 로직을 인라인 복제.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT config_key, config_value, priority
        FROM amazon_tv_config
        WHERE category = 'xpath' AND is_active = TRUE
        ORDER BY config_key, priority
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    groups = {}
    for config_key, config_value, priority in rows:
        m = re.match(r'^(.+?)_(\d+)$', config_key)
        prefix = m.group(1) if m else config_key
        groups.setdefault(prefix, []).append((priority, config_value))

    xpaths = {}
    for prefix, entries in groups.items():
        entries.sort(key=lambda x: x[0])
        xpaths[prefix] = [v for _, v in entries]

    print(f"[INFO] Loaded {sum(len(v) for v in xpaths.values())} XPaths in {len(xpaths)} groups")
    return xpaths


def lazy_load_reviews(page):
    """dt1._lazy_load_reviews 인라인 사본."""
    try:
        page.scroll.to_bottom()
        prev_cards = -1
        stable_count = 0
        for _ in range(8):
            time.sleep(1)
            cards = page.run_js(
                'return document.querySelectorAll(\'[id^="customer_review-"], [id^="customer_review_foreign-"]\').length'
            ) or 0
            if cards == prev_cards and cards > 0:
                stable_count += 1
                if stable_count >= 2:
                    break
            else:
                stable_count = 0
                prev_cards = cards
        print(f"  [DEBUG] lazy-load: {prev_cards if prev_cards >= 0 else 0} review cards loaded")
    except Exception as e:
        print(f"  [WARNING] lazy-load scroll failed: {e}")


def extract_summarized_review(tree, xpaths):
    """dt1.extract_summarized_review 인라인 사본."""
    try:
        paths = xpaths.get('summarized_review') or [
            '//div[@data-testid="overall-summary"]//span[contains(@class, "__SAR2l0zNyyuZ")]',
            '//*[@id="product-summary"]/p[1]/span',
            '//div[@data-testid="overall-summary"]//span',
        ]
        for xpath in paths:
            elements = tree.xpath(xpath)
            for el in elements:
                text = el.text_content() if hasattr(el, 'text_content') else str(el)
                text = ' '.join(text.split())
                if text:
                    return text
        return None
    except Exception:
        return None


def extract_detailed_reviews(tree, xpaths):
    """dt1.extract_detailed_reviews 인라인 사본 (DB priority list, lxml 기반)."""
    try:
        body_xpaths = xpaths.get('review_body_detail') or []
        if not body_xpaths:
            print(f"  [WARNING] review_body_detail XPath not in DB")
            return None

        review_texts = []
        for xpath in body_xpaths:
            elements = tree.xpath(xpath)
            if not elements:
                continue
            for el in elements:
                text = el.text_content() if hasattr(el, 'text_content') else str(el)
                text = ' '.join(text.split())
                text = re.sub(r'\s*Read more\s*$', '', text, flags=re.IGNORECASE)
                if text and len(text) > 5:
                    review_texts.append(text)
            if review_texts:
                break

        if not review_texts:
            print(f"  [DEBUG] No reviews extracted")
            return None

        seen = set()
        deduped = []
        for t in review_texts:
            if t not in seen:
                seen.add(t)
                deduped.append(t)

        print(f"  [DEBUG] extracted reviews: {len(deduped)}")
        formatted = [f"review{i} - {r}" for i, r in enumerate(deduped, 1)]
        return ' ||| '.join(formatted)

    except Exception as e:
        print(f"  [WARNING] Failed to extract detailed reviews: {e}")
        return None


def test_one(page, xpaths, idx, total, url):
    print()
    print('=' * 80)
    print(f'[TEST {idx}/{total}] {url[:100]}...')
    print('=' * 80)

    try:
        page.get(url)
        time.sleep(random.uniform(3, 5))

        lazy_load_reviews(page)
        tree = html.fromstring(page.html)

        summary = extract_summarized_review(tree, xpaths)
        detailed = extract_detailed_reviews(tree, xpaths)

        print()
        print('--- Summarized_Review_Content ---')
        print(summary if summary else '(NULL)')

        print()
        print('--- Detailed_Review_Content ---')
        if detailed:
            count = len(detailed.split(' ||| '))
            print(f'(extracted {count} reviews)')
            print(detailed)
        else:
            print('(NULL)')

    except Exception as e:
        print(f'[ERROR] {e}')
        traceback.print_exc()


def main():
    xpaths = load_xpaths_from_db()

    print('[INFO] Setting up DrissionPage browser (anonymous, no cookies)...')
    co = ChromiumOptions()
    co.set_argument('--lang=en-US')
    page = ChromiumPage(co)
    page.set.window.max()
    print('[OK] Browser ready')

    try:
        for idx, url in enumerate(TEST_URLS, 1):
            test_one(page, xpaths, idx, len(TEST_URLS), url)
    finally:
        try:
            page.quit()
        except Exception:
            pass


if __name__ == '__main__':
    main()
