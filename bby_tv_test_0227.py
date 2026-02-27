"""
bby_tv_dt1.py 가격 추출 테스트 (See price in cart)
- DB 저장 없음, 로그 출력만
"""
import time
import re
from DrissionPage import ChromiumPage, ChromiumOptions
from lxml import html
from bby_config_loader import get_config

TEST_URL = 'https://www.bestbuy.com/product/samsung-55-class-qn70f-series-neo-qled-mini-led-4k-uhd-samsungvision-ai-smart-tizen-tv-2025/J3ZYG2F963'


def main():
    config = get_config()
    file_name = 'bby_tv_dt1'

    # 브라우저 설정
    print('[INFO] Setting up browser...')
    co = ChromiumOptions()
    co.no_imgs(True)
    page = ChromiumPage(co)
    print('[OK] Browser ready')

    try:
        # 페이지 접속
        print(f'\n{"="*80}')
        print(f'[TEST] Accessing: {TEST_URL}')
        page.get(TEST_URL)

        # h1 로딩 대기
        h1_elem = page.ele('xpath://h1[contains(@class, "h4") or contains(@class, "heading")]', timeout=20)
        if h1_elem:
            print(f'[OK] Page loaded - Title: {h1_elem.text[:80]}')
        else:
            print('[ERROR] h1 not found - page load failed')
            return

        # 가격 컨테이너 로딩 대기
        try:
            price_elem = page.ele(
                'xpath://div[@data-testid="price-block-customer-price"] | //div[@data-testid="price-restricted-price-tap-for-price"] | //div[contains(text(), "no longer available in new condition")]',
                timeout=10
            )
            if price_elem:
                print(f'[OK] Price element loaded: {price_elem.tag} / {price_elem.attr("data-testid") or price_elem.text[:50]}')
        except Exception:
            print('[WARNING] Price element loading timeout')

        # HTML tree 생성
        page_source = page.html
        tree = html.fromstring(page_source)

        # ============================================================
        # 1. extract_final_sku_price 로직 재현
        # ============================================================
        print(f'\n{"="*80}')
        print('[TEST] extract_final_sku_price 트레이싱')
        print(f'{"="*80}')

        # 0단계: no longer available
        no_longer_available_xpaths = config.get_xpath_list('no_longer_available', file_name) or [
            '//div[@class="text-danger text-4 font-500 leading-4"]',
            '//div[contains(@class, "text-danger")][contains(text(), "no longer available")]',
            '//div[contains(text(), "This item is no longer available in new condition")]'
        ]
        no_longer = False
        for i, xpath in enumerate(no_longer_available_xpaths):
            elem = tree.xpath(xpath)
            if elem:
                text = elem[0].text_content().strip()
                print(f'  [0단계] xpath[{i}] 매칭: "{text[:60]}"')
                if "no longer available" in text.lower():
                    print(f'  [0단계] → "no longer available" 감지')
                    no_longer = True
                    break
        if not no_longer:
            print(f'  [0단계] no longer available: 해당 없음 ✓')

        # 1단계: 컨테이너 찾기
        container_xpaths = config.get_xpath_list('price_block_container', file_name) or [
            '//div[@data-testid="price-block"]',
            '//div[contains(@class, "order-2")]'
        ]
        price_container = None
        for i, xpath in enumerate(container_xpaths):
            containers = tree.xpath(xpath)
            print(f'  [1단계] xpath[{i}] "{xpath}" → {len(containers)}개 매칭')
            if containers and price_container is None:
                price_container = containers[0]
                # 컨테이너 정보 출력
                attrs = dict(price_container.attrib)
                print(f'  [1단계] → 선택된 컨테이너: <{price_container.tag} {attrs}>')
                print(f'  [1단계] → 자식 element 수: {len(price_container)}, text_content 길이: {len(price_container.text_content())}')

        if price_container is None:
            print(f'  [1단계] ❌ 컨테이너 못 찾음')
            return

        # 2단계: price_xpaths
        price_xpaths = config.get_xpath_list('final_price_inner', file_name) or [
            './/div[@data-testid="price-block-customer-price"]//span',
            './/div[@data-lu-target="customer_price"]//span',
            './/span[@class="font-sans text-default text-style-body-md-400 font-500 text-7 leading-7"]'
        ]
        price_found = False
        for i, xpath in enumerate(price_xpaths):
            elem = price_container.xpath(xpath)
            if elem:
                text = elem[0].text_content().strip()
                print(f'  [2단계] xpath[{i}] 매칭 → "{text}"')
                if text and '$' in text:
                    print(f'  [2단계] → ✅ final_sku_price = "{text}"')
                    price_found = True
                    break
                else:
                    print(f'  [2단계] → $ 없음, skip')
            else:
                print(f'  [2단계] xpath[{i}] 매칭 없음')

        # Fallback: See price in cart
        if not price_found:
            see_price_xpaths = config.get_xpath_list('see_price_in_cart', file_name) or [
                './/div[@data-testid="price-restricted-price-tap-for-price"]//span',
                './/span[contains(text(), "See price in cart")]',
                './/span[contains(text(), "See details in checkout")]'
            ]
            for i, xpath in enumerate(see_price_xpaths):
                elem = price_container.xpath(xpath)
                if elem:
                    text = elem[0].text_content().strip()
                    print(f'  [Fallback] xpath[{i}] 매칭 → "{text}"')
                    if "See price in cart" in text:
                        print(f'  [Fallback] → ✅ final_sku_price = "See price in cart"')
                        price_found = True
                        break
                    elif "See details in checkout" in text:
                        print(f'  [Fallback] → ✅ final_sku_price = "See details in checkout"')
                        price_found = True
                        break
                else:
                    print(f'  [Fallback] xpath[{i}] 매칭 없음')

        if not price_found:
            print(f'  [결과] ❌ final_sku_price 추출 실패 (None)')

        # ============================================================
        # 2. extract_savings 로직 재현
        # ============================================================
        print(f'\n{"="*80}')
        print('[TEST] extract_savings 트레이싱')
        print(f'{"="*80}')

        savings_xpaths = config.get_xpath_list('savings_inner', file_name) or [
            './/span[@data-testid="price-block-total-savings-text"]',
            './/div[@data-testid="price-block-total-savings"]//span',
            './/span[contains(@style, "color: rgb(232, 30, 37)") and contains(., "Save")]'
        ]
        savings = None
        for i, xpath in enumerate(savings_xpaths):
            elem = price_container.xpath(xpath)
            if elem:
                text = elem[0].text_content().strip()
                match = re.search(r'\$[\d,]+(?:\.\d{2})?', text)
                if match:
                    savings = match.group()
                    print(f'  xpath[{i}] 매칭 → "{text}" → ✅ savings = "{savings}"')
                    break
            else:
                print(f'  xpath[{i}] 매칭 없음')
        if savings is None:
            print(f'  [결과] savings = None (할인 없음)')

        # ============================================================
        # 3. extract_original_sku_price 로직 재현
        # ============================================================
        print(f'\n{"="*80}')
        print('[TEST] extract_original_sku_price 트레이싱')
        print(f'{"="*80}')

        orig_xpaths = config.get_xpath_list('original_price_inner', file_name) or [
            './/span[@data-lu-target="comp_value"]',
            './/span[@data-testid="price-block-regular-price-message-text"]//span[@data-lu-target="comp_value"]',
            './/span[contains(@style, "color: rgb(108, 111, 117)") and contains(., "$")]'
        ]
        original_price = None
        for i, xpath in enumerate(orig_xpaths):
            elem = price_container.xpath(xpath)
            if elem:
                text = elem[0].text_content().strip()
                if text and '$' in text:
                    original_price = text
                    print(f'  xpath[{i}] 매칭 → ✅ original_sku_price = "{text}"')
                    break
            else:
                print(f'  xpath[{i}] 매칭 없음')
        if original_price is None:
            print(f'  [결과] original_sku_price = None (세일 아님)')

        # ============================================================
        # 최종 결과 요약
        # ============================================================
        print(f'\n{"="*80}')
        print(f'[결과 요약]')
        print(f'{"="*80}')
        print(f'  URL: {TEST_URL}')
        print(f'  final_sku_price:    {price_found and "추출 성공" or "❌ 실패"}')
        print(f'  savings:            {savings}')
        print(f'  original_sku_price: {original_price}')
        print(f'{"="*80}')

    except Exception as e:
        print(f'[ERROR] {e}')
        import traceback
        traceback.print_exc()
    finally:
        page.quit()
        print('[INFO] Browser closed')


if __name__ == '__main__':
    main()
