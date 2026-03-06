"""
Amazon TV Recovery Script - 20260306_020914 배치 수집 실패 건 재수집 + DB UPDATE
amazon_tv_detail_crawled, tv_retail_com 두 테이블 모두 업데이트
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

TARGET_BATCH_ID = '20260306_020914'

RECOVERY_URLS = [
    "https://www.amazon.com/Hisense-32-Inch-Class-32A4HNF-Built/dp/B0FN6KJVWJ/ref=zg_bs_g_172659_d_sccl_20/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Hisense-Cinema-Hi-QLED-Smart-43E6QF/dp/B0FHL66FPY/ref=zg_bs_g_172659_d_sccl_21/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Samsung-55-Inch-Processor-Picture-Quality/dp/B0DXN3QKS8/ref=zg_bs_g_172659_d_sccl_22/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Hisense-55-Inch-CanvasTV-Vision-55S7N/dp/B0D2PNH494/ref=zg_bs_g_172659_d_sccl_23/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Westinghouse-Roku-Connectivity-Compatible-Assistant/dp/B0BZTB81QV/ref=zg_bs_g_172659_d_sccl_25/132-8367918-3697711?psc=1",
    "https://www.amazon.com/TCL-55QM6K-120HZ-144HZ-Brightness-Television/dp/B0DSR9CHB1/ref=zg_bs_g_172659_d_sccl_26/132-8367918-3697711?psc=1",
    "https://www.amazon.com/hisense-fire-tv-43-inch-class-A7NF-series-qled-smart-tv/dp/B0DN6THH67/ref=zg_bs_g_172659_d_sccl_27/132-8367918-3697711?psc=1",
    "https://www.amazon.com/INSIGNIA-75-inch-Class-Remote-NS75-UQFL26/dp/B0DWBMW3LD/ref=zg_bs_g_172659_d_sccl_28/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Hisense-Class-Mini-LED-55U65QF-Built/dp/B0DYWG3BL1/ref=zg_bs_g_172659_d_sccl_29/132-8367918-3697711?psc=1",
    "https://www.amazon.com/SAMSUNG-65-Inch-Processor-Upscaling-Xcelerator/dp/B0DXMJGQWC/ref=zg_bs_g_172659_d_sccl_31/132-8367918-3697711?psc=1",
    "https://www.amazon.com/INSIGNIA-24-inch-LED-HD-Fire-TV/dp/B0F9Z3H6YG/ref=zg_bs_g_172659_d_sccl_32/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Hisense-Class-Mini-LED-Google-55U75QG/dp/B0F22F1VLH/ref=zg_bs_g_172659_d_sccl_33/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Sony-85-Inch-Exclusive-Features-PlayStation%C2%AE5/dp/B0CVPCLY6X/ref=zg_bs_g_172659_d_sccl_35/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Westinghouse-Parental-Controls-Non-Smart-Monitor/dp/B09QRM1LVN/ref=zg_bs_g_172659_d_sccl_39/132-8367918-3697711?psc=1",
    "https://www.amazon.com/LG-Upscaling-Filmmaker-Orchestra-55QNED82AUA/dp/B0F1PDTQY2/ref=zg_bs_g_172659_d_sccl_40/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Hisense-Class-Mini-LED-Google-65U8QG/dp/B0F1DV217B/ref=zg_bs_g_172659_d_sccl_42/132-8367918-3697711?psc=1",
    "https://www.amazon.com/VIZIO-D24fM-K01-TBD/dp/B0B286BGSL/ref=zg_bs_g_172659_d_sccl_43/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Westinghouse-24-inch-Television-Bluetooth-Connectivity/dp/B0FC34R88H/ref=zg_bs_g_172659_d_sccl_45/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Samsung-65-Inch-Processor-Technology-Xcelerator/dp/B0DXN42NXS/ref=zg_bs_g_172659_d_sccl_46/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Samsung-65-Inch-65QN90F-Quantum-Tracking/dp/B0DXMYSQJC/ref=zg_bs_g_172659_d_sccl_47/132-8367918-3697711?psc=1",
    "https://www.amazon.com/ONN-32-inch-Connectivity-Compatible-Assistant/dp/B0G4B9MNCQ/ref=zg_bs_g_172659_d_sccl_49/132-8367918-3697711?psc=1",
    "https://www.amazon.com/TCL-55QM7K-120HZ-144HZ-Reflective-Television/dp/B0DVWXXRDL/ref=zg_bs_g_172659_d_sccl_50/132-8367918-3697711?psc=1",
    "https://www.amazon.com/INSIGNIA-fire-tv-65-inch-class-f50-series-4k-smart-tv/dp/B0DM2LQV3F/ref=zg_bs_g_172659_d_sccl_1/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Sony-Exclusive-Features-PlayStation%C2%AE5-K-43S20M2/dp/B0DYKBNW89/ref=zg_bs_g_172659_d_sccl_2/132-8367918-3697711?psc=1",
    "https://www.amazon.com/LG-Upscaling-Filmmaker-Orchestra-OLED65G5WUA/dp/B0DYQR8R98/ref=zg_bs_g_172659_d_sccl_3/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Amazon-Vision-Ambient-Experience-hands-free/dp/B0CJDH5L8Y/ref=zg_bs_g_172659_d_sccl_6/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Westinghouse-Roku-Connectivity-Compatible-Assistant/dp/B0DKVKMZX9/ref=zg_bs_g_172659_d_sccl_8/132-8367918-3697711?psc=1",
    "https://www.amazon.com/SAMSUNG-Tracking-Xcelerator-Q-Symphony-UN65DU7200/dp/B0CVS1XHJL/ref=zg_bs_g_172659_d_sccl_9/132-8367918-3697711?psc=1",
    "https://www.amazon.com/FPD-Bluetooth-Streaming-Television-CG43-C3/dp/B0DJQJ6T68/ref=zg_bs_g_172659_d_sccl_10/132-8367918-3697711?psc=1",
    "https://www.amazon.com/amazon-fire-tv-50-inch-4-series-4k-smart-tv/dp/B0CZBLZYY5/ref=zg_bs_g_172659_d_sccl_12/132-8367918-3697711?psc=1",
    "https://www.amazon.com/Feihe-Television-Kitchen-Bedroom-Entertainment/dp/B0F8C3VTVY/ref=zg_bs_g_172659_d_sccl_13/132-8367918-3697711?psc=1",
    "https://www.amazon.com/FPD-32-inch-Chromecast-Palette-CG32-P3/dp/B0CRRD2DV4/ref=zg_bs_g_172659_d_sccl_15/132-8367918-3697711?psc=1",
    "https://www.amazon.com/LG-34SR63QA-W-Streaming-UltraWide-Bluetooth/dp/B0DP14TVLY/ref=zg_bs_g_172659_d_sccl_16/132-8367918-3697711?psc=1",
    "https://www.amazon.com/LG-65-Inch-Processor-AI-Powered-OLED65C4PUA/dp/B0CVS18PH9/ref=zg_bs_g_172659_d_sccl_17/132-8367918-3697711?psc=1",
    "https://www.amazon.com/SAMSUNG-55-Inch-Anti-Reflection-Customizable-QN55LS03D/dp/B0CV9F2Q5V/ref=zg_bs_g_172659_d_sccl_18/132-8367918-3697711?psc=1",
    "https://www.amazon.com/LG-Upscaling-Filmmaker-Orchestra-OLED55B5PUA-AUSZ/dp/B0FFXN5PN9/ref=zg_bs_g_172659_d_sccl_19/132-8367918-3697711?psc=1",
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


def load_xpaths_from_db(conn):
    """amazon_tv_config에서 xpath 로드"""
    cur = conn.cursor()
    cur.execute("""
        SELECT config_key, config_value, priority
        FROM amazon_tv_config
        WHERE category = 'xpath' AND is_active = TRUE
        ORDER BY config_key, priority
    """)
    rows = cur.fetchall()
    cur.close()

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


def extract_with_xpaths(tree, xpaths, key, fallback=None):
    xpath_list = xpaths.get(key) or fallback or []
    for xpath in xpath_list:
        result = extract_text_safe(tree, xpath)
        if result:
            return result
    return None


def extract_star_rating(tree, xpaths):
    star_rating_xpaths = xpaths.get('star_rating') or [
        '//*[@id="acrPopover"]/@title',
        '//*[@id="acrPopover"]/span[1]/a/span'
    ]
    for xpath in star_rating_xpaths:
        text = extract_text_safe(tree, xpath)
        if text:
            match = re.search(r'(\d+\.?\d*)\s*out of\s*5', text)
            if match:
                return match.group(1)
            if re.search(r'\d', text) and "No customer reviews" not in text:
                return text

    no_reviews_xpaths = xpaths.get('no_reviews') or [
        '//*[@id="cm-cr-dp-review-header"]/h3/span',
        '//span[contains(text(), "No customer reviews")]'
    ]
    for xpath in no_reviews_xpaths:
        text = extract_text_safe(tree, xpath)
        if text and "No customer reviews" in text:
            return "No customer reviews"
    return None


def extract_count_of_star_ratings(tree, xpaths):
    cosr_xpaths = xpaths.get('count_of_star_ratings') or [
        '//*[@id="acrCustomerReviewText"]'
    ]
    for xpath in cosr_xpaths:
        text = extract_text_safe(tree, xpath)
        if text:
            m = re.search(r'([\d,]+)\s*global ratings?', text)
            if m:
                return int(m.group(1).replace(',', ''))
            m = re.search(r'([\d,]+)\s*ratings?', text)
            if m:
                return int(m.group(1).replace(',', ''))
            m = re.search(r'([\d,]+)', text)
            if m:
                return int(m.group(1).replace(',', ''))
    return None


def extract_final_sku_price(tree, xpaths):
    # Special cases
    for xpath in (xpaths.get('unavailable') or ['//*[@id="outOfStock"]/div/div[1]/span[1]']):
        text = extract_text_safe(tree, xpath)
        if text and 'currently unavailable' in text.lower():
            return "Currently unavailable."

    for xpath in (xpaths.get('price_higher') or []):
        text = extract_text_safe(tree, xpath)
        if text:
            if 'price higher than typical' in text.lower():
                return "Price higher than typical"
            if text.lower().strip() == 'high price':
                return "High price"

    for xpath in (xpaths.get('no_offers') or []):
        text = extract_text_safe(tree, xpath)
        if text and 'no featured offers available' in text.lower():
            return "No featured offers available"

    price_xpaths = xpaths.get('price') or [
        '//*[@id="corePriceDisplay_desktop_feature_div"]/div[1]/span[1]',
        '//*[@id="corePrice_feature_div"]/div/div/span[1]/span[1]',
    ]
    for xpath in price_xpaths:
        text = extract_text_safe(tree, xpath)
        if text:
            m = re.search(r'\$[\d,]+\.?\d*', text)
            if m:
                return m.group()
    return None


def extract_original_sku_price(tree, xpaths):
    osp_xpaths = xpaths.get('original_price') or [
        '//*[@id="corePriceDisplay_desktop_feature_div"]/div[2]/span/span[1]/span[2]/span/span[1]',
        '//*[@id="corePriceDisplay_desktop_feature_div"]/div[2]//span[@class="a-offscreen"]'
    ]
    for xpath in osp_xpaths:
        text = extract_text_safe(tree, xpath)
        if text:
            return text.strip()
    return None


def calculate_savings(final_price, original_price):
    try:
        if not final_price or not original_price:
            return None
        final_match = re.search(r'[\d,]+\.?\d*', final_price.replace(',', ''))
        original_match = re.search(r'[\d,]+\.?\d*', original_price.replace(',', ''))
        if final_match and original_match:
            final_value = float(final_match.group())
            original_value = float(original_match.group())
            savings = original_value - final_value
            if savings > 0:
                return f"${savings:.2f}"
        return None
    except:
        return None


def extract_summarized_review(page, xpaths):
    """AI 요약 리뷰 추출 (DrissionPage - JS 동적 로딩 요소)"""
    try:
        sr_xpaths = xpaths.get('summarized_review') or [
            '//div[@data-testid="overall-summary"]//span[contains(@class, "__SAR2l0zNyyuZ")]',
            '//*[@id="product-summary"]/p[1]/span',
            '//div[@data-testid="overall-summary"]//span'
        ]
        for xpath in sr_xpaths:
            try:
                elem = page.ele(f'xpath:{xpath}', timeout=10)
                if elem and elem.text and elem.text.strip():
                    return elem.text.strip()
            except Exception:
                continue
        return None
    except Exception as e:
        print(f"  [WARNING] Failed to extract summarized review: {e}")
        return None


def extract_detailed_reviews(page, xpaths):
    """상품 페이지에서 detailed_review_content 추출 (JS → fallback XPath)"""
    try:
        # 페이지 하단까지 스크롤 (lazy loading 트리거)
        for _ in range(5):
            page.run_js("window.scrollBy(0, window.innerHeight)")
            time.sleep(0.5)
        time.sleep(2)

        # JS로 리뷰 텍스트 추출
        js_code = """
        var reviews = [];
        var containers = document.querySelectorAll('[id^="customer_review-"]');
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

        # JS 실패 시 fallback: XPath (DB 로드)
        if not review_texts:
            fallback_xpaths = xpaths.get('review_body_detail') or [
                '//*[starts-with(@id, "customer_review-")]/div[4]/span/div/div[1]/span',
                '//*[starts-with(@id, "customer_review-")]/div[4]/span/div/div[1]',
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
                            break
                except Exception:
                    continue

        if not review_texts:
            return None

        all_reviews = []
        collected_reviews = set()
        for text in review_texts:
            review_text = ' '.join(text.split())
            review_text = re.sub(r'\s*Read more\s*$', '', review_text, flags=re.IGNORECASE)
            if review_text and len(review_text) > 5 and review_text not in collected_reviews:
                all_reviews.append(review_text)
                collected_reviews.add(review_text)

        if all_reviews:
            formatted_reviews = [f"review{idx} - {review}" for idx, review in enumerate(all_reviews, 1)]
            return ' ||| '.join(formatted_reviews)
        return None

    except Exception as e:
        print(f"  [WARNING] Failed to extract detailed reviews: {e}")
        return None


def extract_asin(url):
    match = re.search(r'/dp/([A-Z0-9]{10})', url)
    return match.group(1) if match else None


def main():
    print("=" * 80)
    print(f"Amazon TV Recovery - batch {TARGET_BATCH_ID}")
    print(f"Total URLs: {len(RECOVERY_URLS)}")
    print("=" * 80)

    # ASIN 중복 제거
    asin_url_map = {}
    for url in RECOVERY_URLS:
        asin = extract_asin(url)
        if asin and asin not in asin_url_map:
            asin_url_map[asin] = url
    unique_urls = list(asin_url_map.items())
    print(f"Unique ASINs: {len(unique_urls)}")

    # DB 연결
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    # XPath 로드
    xpaths = load_xpaths_from_db(conn)
    total_xpaths = sum(len(v) for v in xpaths.values())
    print(f"[OK] Loaded {total_xpaths} XPaths in {len(xpaths)} groups")

    # 브라우저
    co = ChromiumOptions()
    co.set_argument('--lang=en-US')
    page = ChromiumPage(co)
    page.set.window.max()
    print("[OK] Browser ready\n")

    updated = 0
    failed = 0

    for i, (asin, url) in enumerate(unique_urls, 1):
        print(f"--- [{i}/{len(unique_urls)}] {asin} ---")

        try:
            page.get(url)
            time.sleep(3)
            page.ele('#productTitle', timeout=10)
            page.ele('xpath://*[@id="acrPopover"] | //*[@id="averageCustomerReviews"]', timeout=5)

            tree = html.fromstring(page.html)

            retailer_sku_name = extract_with_xpaths(tree, xpaths, 'product_name', ['//*[@id="productTitle"]'])

            # retailer_sku_name NULL이면 refresh 재시도
            if not retailer_sku_name:
                print(f"  [RETRY] retailer_sku_name NULL - refreshing...")
                page.refresh()
                time.sleep(10)
                tree = html.fromstring(page.html)
                retailer_sku_name = extract_with_xpaths(tree, xpaths, 'product_name', ['//*[@id="productTitle"]'])

            sr = extract_star_rating(tree, xpaths)
            cosr = extract_count_of_star_ratings(tree, xpaths)
            fsp = extract_final_sku_price(tree, xpaths)
            osp = extract_original_sku_price(tree, xpaths)

            # special price면 osp=None
            special_prices = ["Currently unavailable.", "Price higher than typical",
                              "High price", "No featured offers available",
                              "See price in cart", "To see our price, add this item to your cart."]
            if fsp in special_prices:
                osp = None

            # fsp > osp면 osp=None
            if fsp and osp:
                try:
                    f_match = re.search(r'[\d.]+', fsp.replace(',', ''))
                    o_match = re.search(r'[\d.]+', osp.replace(',', ''))
                    if f_match and o_match and float(f_match.group()) > float(o_match.group()):
                        osp = None
                except:
                    pass

            savings = calculate_savings(fsp, osp)

            # summarized_review_content 추출
            summarized_review = extract_summarized_review(page, xpaths)

            # detailed_review_content 추출
            detailed_review = extract_detailed_reviews(page, xpaths)

            # No customer reviews 처리
            if sr == "No customer reviews":
                cosr = 0

            print(f"  name={'OK' if retailer_sku_name else 'NULL'}, sr={sr!r}, cosr={cosr!r}, fsp={fsp!r}, osp={osp!r}, savings={savings!r}, summary={'OK' if summarized_review else 'NULL'}, review={'OK' if detailed_review else 'NULL'}")

            # DB UPDATE - amazon_tv_detail_crawled
            cur = conn.cursor()
            cur.execute("""
                UPDATE amazon_tv_detail_crawled
                SET retailer_sku_name = COALESCE(%s, retailer_sku_name),
                    star_rating = COALESCE(%s, star_rating),
                    count_of_star_ratings = COALESCE(%s, count_of_star_ratings),
                    final_sku_price = COALESCE(%s, final_sku_price),
                    original_sku_price = COALESCE(%s, original_sku_price),
                    summarized_review_content = COALESCE(%s, summarized_review_content),
                    detailed_review_content = COALESCE(%s, detailed_review_content)
                WHERE batch_id = %s AND item = %s
            """, (retailer_sku_name, sr, str(cosr) if cosr is not None else None,
                  fsp, osp, summarized_review, detailed_review, TARGET_BATCH_ID, asin))
            detail_rows = cur.rowcount

            # DB UPDATE - tv_retail_com (crawl_datetime >= '2026-03-05 12:09:57' 이후, NULL 필드 있는 row만)
            cur.execute("""
                UPDATE tv_retail_com
                SET retailer_sku_name = COALESCE(%s, retailer_sku_name),
                    star_rating = COALESCE(%s, star_rating),
                    count_of_star_ratings = COALESCE(%s, count_of_star_ratings),
                    count_of_reviews = NULL,
                    final_sku_price = COALESCE(%s, final_sku_price),
                    original_sku_price = COALESCE(%s, original_sku_price),
                    savings = COALESCE(%s, savings),
                    summarized_review_content = COALESCE(%s, summarized_review_content),
                    detailed_review_content = COALESCE(%s, detailed_review_content)
                WHERE account_name = 'Amazon' AND item = %s
                AND crawl_datetime >= '2026-03-05 12:09:57'
                AND (final_sku_price IS NULL OR detailed_review_content IS NULL
                     OR summarized_review_content IS NULL
                     OR retailer_sku_name IS NULL OR count_of_star_ratings IS NULL
                     OR count_of_reviews IS NULL OR star_rating IS NULL)
            """, (retailer_sku_name, sr, str(cosr) if cosr is not None else None,
                  fsp, osp, savings, summarized_review, detailed_review, asin))
            retail_rows = cur.rowcount

            cur.close()

            print(f"  [DB] detail_crawled: {detail_rows} rows, tv_retail_com: {retail_rows} rows updated")
            updated += 1

        except Exception as e:
            print(f"  [ERROR] {e}")
            failed += 1

        print()

    # 결과 요약
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"  Total: {len(unique_urls)}, Updated: {updated}, Failed: {failed}")

    # 업데이트 후 NULL 현황
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) as total,
            COUNT(*) FILTER (WHERE star_rating IS NULL) as sr_null,
            COUNT(*) FILTER (WHERE count_of_star_ratings IS NULL) as cosr_null,
            COUNT(*) FILTER (WHERE final_sku_price IS NULL) as fsp_null,
            COUNT(*) FILTER (WHERE retailer_sku_name IS NULL) as rsn_null
        FROM amazon_tv_detail_crawled
        WHERE batch_id = %s
    """, (TARGET_BATCH_ID,))
    r = cur.fetchone()
    print(f"  After recovery - total={r[0]}, sr_null={r[1]}, cosr_null={r[2]}, fsp_null={r[3]}, rsn_null={r[4]}")
    cur.close()

    conn.close()
    page.quit()


if __name__ == '__main__':
    main()
