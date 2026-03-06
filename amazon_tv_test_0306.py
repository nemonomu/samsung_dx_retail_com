"""
Amazon TV star_rating / count_of_star_ratings / count_of_reviews 추출 테스트
DB 저장 없이 XPath 로드 + 페이지 추출 + 로그 출력만 수행
"""
import sys
import time
import re
import psycopg2
from DrissionPage import ChromiumPage, ChromiumOptions
from lxml import html

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from config import DB_CONFIG

TEST_URLS = [
    "https://www.amazon.com/insignia-fire-tv-50-inch-class-f30-series-4k-smart-tv/dp/B0BTTVRWPR/ref=zg_bs_g_172659_d_sccl_21/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Sony-Exclusive-Features-PlayStation%C2%AE-K-65XR8B/dp/B0FHXY8DW4/ref=zg_bs_g_172659_d_sccl_23/132-8367918-3697711?psc=1",
    "https://www.amazon.com/KTC-Portable-Touchscreen-Certification-A25Q5/dp/B0F5WPDTCZ/ref=zg_bs_g_172659_d_sccl_24/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Roku-Brilliant-Automatic-Brightness-Streaming/dp/B0CLFSWK9V/ref=zg_bs_g_172659_d_sccl_26/132-8367918-3697711?psc=1",
    "https://www.amazon.com/LG-Upscaling-Filmmaker-Orchestra-65QNED85AUA/dp/B0DYQHCVFZ/ref=zg_bs_g_172659_d_sccl_27/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Samsung-55-Inch-55QN80F-Tracking-Processor/dp/B0DXN7BRLL/ref=zg_bs_g_172659_d_sccl_28/132-8367918-3697711?psc=1",
    "https://www.amazon.com/SAMSUNG-32-Inch-Tracking-Xcelerator-QN32Q60D/dp/B0CV9MGX22/ref=zg_bs_g_172659_d_sccl_29/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Atyme-320GM5HD-ATYME-LED-HDTV/dp/B07FYYYHNP/ref=zg_bs_g_172659_d_sccl_30/132-8367918-3697711?psc=1",
    "https://www.amazon.com/amazon-fire-tv-55-inch-omni-mini-led-series-smart-tv/dp/B0C7SRHGXF/ref=zg_bs_g_172659_d_sccl_81/132-8367918-3697711?psc=1",
    "https://www.amazon.com/TCL-65QM8K-120HZ-144HZ-Reflective-Television/dp/B0F53CZ4WT/ref=zg_bs_g_172659_d_sccl_82/132-8367918-3697711?psc=1",
    "https://www.amazon.com/SAMSUNG-65-Inch-Processor-Xcelerator-Samsung/dp/B0DXMJFJ7W/ref=zg_bs_g_172659_d_sccl_83/132-8367918-3697711?psc=1",
    "https://www.amazon.com/SAMSUNG-Tracking-Xcelerator-Enhancer-QN43QN90D/dp/B0CV9RV77Z/ref=zg_bs_g_172659_d_sccl_84/132-8367918-3697711?psc=1",
    "https://www.amazon.com/LG-Upscaling-Filmmaker-Compatible-55UA7700PUB/dp/B0F5SFM2MR/ref=zg_bs_g_172659_d_sccl_85/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Panasonic-TV-65W70BP-Essentials-Protection-BEACH-CPS-26M1000ATV/dp/B0FJ6KKBFY/ref=zg_bs_g_172659_d_sccl_86/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Roku-Smart-2025-Television-Entertainment/dp/B0DWGKMNND/ref=zg_bs_g_172659_d_sccl_87/132-8367918-3697711?psc=1",
    "https://www.amazon.com/insignia-fire-tv-43-inch-class-f30-series-4k-smart-tv/dp/B0CMDJ8TK3/ref=zg_bs_g_172659_d_sccl_88/132-8367918-3697711?psc=1",
    "https://www.amazon.com/SAMSUNG-65-Inch-Tracking-Xcelerator-UN65DU8000/dp/B0CV9G5ST8/ref=zg_bs_g_172659_d_sccl_89/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Samsung-Resolution-SmartThings-Compatible-LH55BEFHLGFXGO/dp/B0FPT58XMD/ref=zg_bs_g_172659_d_sccl_91/132-8367918-3697711?psc=1",
    "https://www.amazon.com/toshiba-fire-tv-50-inch-class-c350-series-4k-smart-tv/dp/B0BMK5B4TF/ref=zg_bs_g_172659_d_sccl_92/132-8367918-3697711?psc=1",
    "https://www.amazon.com/VIZIO-720P-Smart-Dual-Band-WiFi/dp/B0D81P3D79/ref=zg_bs_g_172659_d_sccl_93/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Roku-43-Inch-Select-UHD-Smart/dp/B0FGLDZRGV/ref=zg_bs_g_172659_d_sccl_94/132-8367918-3697711?psc=1",
    "https://www.amazon.com/TCL-55-Inch-NXTVISION-Google-Canvas/dp/B0DB6HGXGF/ref=zg_bs_g_172659_d_sccl_95/132-8367918-3697711?psc=1",
    "https://www.amazon.com/SYLVOX-Waterproof-Weatherproof-Television-Chromecast/dp/B0D3H8ZJX3/ref=zg_bs_g_172659_d_sccl_96/132-8367918-3697711?psc=1",
    "https://www.amazon.com/LG-OLED77C5P-inch-Class-Smart/dp/B0F8Y6JT6D/ref=zg_bs_g_172659_d_sccl_97/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Samsung-32-Inch-Smart-LED-Bundle/dp/B0FCZTCJXN/ref=zg_bs_g_172659_d_sccl_98/132-8367918-3697711?psc=1",
    "https://www.amazon.com/SANSUI-Television-chromecast-Compatible-Connection/dp/B0G63J81GS/ref=zg_bs_g_172659_d_sccl_99/132-8367918-3697711?psc=1",
    "https://www.amazon.com/iFFALCON-65-Inch-Class-QD-Mini-Google/dp/B0F7LS3D2Y/ref=zg_bs_g_172659_d_sccl_100/132-8367918-3697711?psc=1",
]


def load_xpaths_from_db():
    """amazon_tv_config에서 xpath 로드"""
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

    xpath_groups = {}
    for config_key, config_value, priority in rows:
        match = re.match(r'^(.+?)_(\d+)$', config_key)
        prefix = match.group(1) if match else config_key
        if prefix not in xpath_groups:
            xpath_groups[prefix] = []
        xpath_groups[prefix].append({'value': config_value, 'priority': priority})

    xpaths = {}
    for prefix, entries in xpath_groups.items():
        sorted_entries = sorted(entries, key=lambda x: x['priority'])
        # 중복 제거 (같은 value는 한 번만)
        seen = set()
        unique = []
        for e in sorted_entries:
            if e['value'] not in seen:
                seen.add(e['value'])
                unique.append(e['value'])
        xpaths[prefix] = unique

    return xpaths


def extract_text_safe(tree, xpath):
    if not xpath:
        return None
    try:
        elements = tree.xpath(xpath)
        if elements:
            if isinstance(elements[0], str):
                return elements[0].strip()
            else:
                return elements[0].text_content().strip()
        return None
    except:
        return None


def extract_star_rating(tree, xpaths):
    star_rating_xpaths = xpaths.get('star_rating') or [
        '//*[@id="acrPopover"]/@title',
        '//*[@id="averageCustomerReviews"]//span[@class="a-icon-alt"]',
        '//span[@data-hook="rating-out-of-text"]',
        '//*[@id="acrPopover"]/span[1]/a/span'
    ]

    for xpath in star_rating_xpaths:
        text = extract_text_safe(tree, xpath)
        if text:
            print(f"    [MATCH] star_rating xpath={xpath} -> text={text!r}")
            match = re.search(r'(\d+\.?\d*)\s*out of\s*5', text)
            if match:
                return match.group(1)
            if re.search(r'\d', text) and "No customer reviews" not in text:
                return text

    # No customer reviews check
    no_reviews_xpaths = xpaths.get('no_reviews') or [
        '//*[@id="cm-cr-dp-review-header"]/h3/span',
        '//span[@data-hook="top-customer-reviews-title"]',
        '//div[@id="cm-cr-dp-review-header"]//span[contains(text(), "No customer reviews")]'
    ]
    for xpath in no_reviews_xpaths:
        text = extract_text_safe(tree, xpath)
        if text and "No customer reviews" in text:
            return "No customer reviews"

    return None


def extract_count_of_star_ratings(tree, xpaths):
    cosr_xpaths = xpaths.get('count_of_star_ratings') or [
        '//*[@id="cm_cr_dp_d_rating_histogram"]/div[3]',
        '//*[@id="acrCustomerReviewText"]',
        '//span[@id="acrCustomerReviewText"]',
        '//a[@id="acrCustomerReviewLink"]//span'
    ]

    for xpath in cosr_xpaths:
        text = extract_text_safe(tree, xpath)
        if text:
            print(f"    [MATCH] count_of_star_ratings xpath={xpath} -> text={text!r}")
            m = re.search(r'([\d,]+)\s*global ratings?', text)
            if m:
                return int(m.group(1).replace(',', ''))
            m = re.search(r'([\d,]+)\s*ratings?', text)
            if m:
                return int(m.group(1).replace(',', ''))
            # Fallback: "(102)" or "102 Reviews" (new Amazon layout 2026-03)
            m = re.search(r'([\d,]+)', text)
            if m:
                return int(m.group(1).replace(',', ''))

    return None


def extract_count_of_reviews(tree, xpaths):
    # Zero reviews check
    zero_xpaths = xpaths.get('zero_reviews') or [
        '//*[@id="reviewsMedley"]//div[@class="a-box-inner"]',
        '//*[@id="reviewsMedley"]/div/div[2]/div/div[2]/div[3]/div[2]/div/div',
        '//div[contains(text(), "customer reviews and")]'
    ]
    for xpath in zero_xpaths:
        text = extract_text_safe(tree, xpath)
        if text:
            m = re.search(r'(\d+)\s*customer\s*reviews?', text, re.IGNORECASE)
            if m and m.group(1) == '0':
                return "0"

    cor_xpaths = xpaths.get('review_count') or [
        '//*[@id="acrCustomerReviewText"]',
        '//span[@id="acrCustomerReviewText"]',
        '//a[@id="acrCustomerReviewLink"]//span'
    ]

    for xpath in cor_xpaths:
        text = extract_text_safe(tree, xpath)
        if text:
            print(f"    [MATCH] count_of_reviews xpath={xpath} -> text={text!r}")
            if "No customer reviews" in text:
                return "0"
            m = re.search(r'([\d,]+)', text)
            if m:
                return m.group(1)

    return None


def extract_final_sku_price(tree, xpaths):
    # Special cases first
    for xpath in (xpaths.get('unavailable') or ['//*[@id="outOfStock"]/div/div[1]/span[1]']):
        text = extract_text_safe(tree, xpath)
        if text and 'currently unavailable' in text.lower():
            return "Currently unavailable."

    for xpath in (xpaths.get('price_higher') or ['//*[@id="fod-cx-message-with-learn-more"]/span[1]']):
        text = extract_text_safe(tree, xpath)
        if text:
            if 'price higher than typical' in text.lower():
                return "Price higher than typical"
            if text.lower().strip() == 'high price':
                return "High price"

    for xpath in (xpaths.get('no_offers') or ['//*[@id="fod-cx-message-with-learn-more"]/span[1]']):
        text = extract_text_safe(tree, xpath)
        if text and 'no featured offers available' in text.lower():
            return "No featured offers available"

    # Normal price extraction
    price_xpaths = xpaths.get('price') or [
        '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]',
        '//*[@id="corePrice_feature_div"]/div/div/span[1]/span[1]',
    ]

    for xpath in price_xpaths:
        text = extract_text_safe(tree, xpath)
        if text:
            print(f"    [MATCH] final_sku_price xpath={xpath} -> text={text!r}")
            m = re.search(r'\$[\d,]+\.?\d*', text)
            if m:
                return m.group()

    return None


def main():
    print("=" * 80)
    print("Amazon TV XPath Test - star_rating / count_of_star_ratings / count_of_reviews / final_sku_price")
    print("=" * 80)

    # 1. DB에서 XPath 로드
    print("\n[1] Loading XPaths from DB...")
    xpaths = load_xpaths_from_db()
    total = sum(len(v) for v in xpaths.values())
    print(f"[OK] Loaded {total} XPaths in {len(xpaths)} groups")

    # 관련 그룹만 출력
    for key in ['star_rating', 'count_of_star_ratings', 'review_count', 'no_reviews', 'zero_reviews', 'price']:
        if key in xpaths:
            print(f"  {key}: {xpaths[key]}")

    # 2. 브라우저 세팅
    print("\n[2] Setting up browser...")
    co = ChromiumOptions()
    co.set_argument('--lang=en-US')
    page = ChromiumPage(co)
    page.set.window.max()
    print("[OK] Browser ready")

    # 3. URL 순회
    print(f"\n[3] Testing {len(TEST_URLS)} URLs...\n")
    results = []

    for i, url in enumerate(TEST_URLS, 1):
        asin = re.search(r'/dp/([A-Z0-9]{10})', url)
        asin = asin.group(1) if asin else '???'
        print(f"--- [{i}/{len(TEST_URLS)}] {asin} ---")

        try:
            page.get(url)
            time.sleep(3)

            # 페이지 로딩 대기
            page.ele('#productTitle', timeout=10)
            page.ele('xpath://*[@id="acrPopover"] | //*[@id="averageCustomerReviews"]', timeout=5)

            page_source = page.html
            tree = html.fromstring(page_source)

            sr = extract_star_rating(tree, xpaths)
            cosr = extract_count_of_star_ratings(tree, xpaths)
            cor = extract_count_of_reviews(tree, xpaths)
            fsp = extract_final_sku_price(tree, xpaths)

            nulls = [k for k, v in [('sr', sr), ('cosr', cosr), ('fsp', fsp)] if v is None]
            status = "OK" if not nulls else f"NULL({','.join(nulls)})"
            print(f"  [RESULT] star_rating={sr!r}, cosr={cosr!r}, cor={cor!r}, fsp={fsp!r} -> {status}")
            results.append({'asin': asin, 'star_rating': sr, 'cosr': cosr, 'cor': cor, 'fsp': fsp})

        except Exception as e:
            print(f"  [ERROR] {e}")
            results.append({'asin': asin, 'star_rating': None, 'cosr': None, 'cor': None, 'fsp': None})

        print()

    # 4. 요약
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    sr_ok = sum(1 for r in results if r['star_rating'] is not None)
    cosr_ok = sum(1 for r in results if r['cosr'] is not None)
    cor_ok = sum(1 for r in results if r['cor'] is not None)
    fsp_ok = sum(1 for r in results if r['fsp'] is not None)
    total = len(results)
    print(f"  star_rating:           {sr_ok}/{total} OK, {total - sr_ok}/{total} NULL")
    print(f"  count_of_star_ratings: {cosr_ok}/{total} OK, {total - cosr_ok}/{total} NULL")
    print(f"  count_of_reviews:      {cor_ok}/{total} OK, {total - cor_ok}/{total} NULL")
    print(f"  final_sku_price:       {fsp_ok}/{total} OK, {total - fsp_ok}/{total} NULL")

    print("\nDetailed results:")
    for r in results:
        nulls = [k for k, v in [('sr', r['star_rating']), ('cosr', r['cosr']), ('fsp', r['fsp'])] if v is None]
        flag = "OK" if not nulls else f"**NULL({','.join(nulls)})**"
        print(f"  {r['asin']}: star={r['star_rating']!r}, cosr={r['cosr']!r}, cor={r['cor']!r}, fsp={r['fsp']!r} {flag}")

    page.quit()


if __name__ == '__main__':
    main()
