"""
Walmart 페이지에서 실제 제품 컨테이너 구조를 분석하는 디버그 스크립트
"""
import psycopg2
from lxml import html

DB_CONFIG = {
    'host': 'samsung-dx-crawl.csnixzmkuppn.ap-northeast-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin2025!'
}

def analyze_containers():
    """여러 XPath 패턴을 시도해서 가장 적합한 패턴 찾기"""

    # 저장된 HTML 파일 읽기 (최근 크롤링한 페이지)
    try:
        with open('walmart_page_debug.html', 'r', encoding='utf-8') as f:
            page_source = f.read()
    except FileNotFoundError:
        print("[ERROR] walmart_page_debug.html 파일이 없습니다.")
        print("[INFO] 크롤러 실행 중 페이지 소스를 저장해주세요.")
        return

    tree = html.fromstring(page_source)

    # 다양한 XPath 패턴 시도
    xpath_candidates = [
        {
            'name': 'Current (strict)',
            'xpath': '//div[contains(@class, "mb0 ph0-xl pt0-xl bb b--near-white w-25 pb3-m ph1")]'
        },
        {
            'name': 'Product title based',
            'xpath': '//div[.//span[@data-automation-id="product-title"]]'
        },
        {
            'name': 'Product price based',
            'xpath': '//div[.//div[@data-automation-id="product-price"]]'
        },
        {
            'name': 'Combined (title + price)',
            'xpath': '//div[.//span[@data-automation-id="product-title"] and .//div[@data-automation-id="product-price"]]'
        },
        {
            'name': 'Search result item',
            'xpath': '//div[@data-item-id]'
        },
        {
            'name': 'Article tag',
            'xpath': '//article'
        },
        {
            'name': 'Wider class pattern',
            'xpath': '//div[contains(@class, "mb0") and contains(@class, "ph1")]'
        }
    ]

    print("="*80)
    print("Walmart Container XPath Analysis")
    print("="*80)

    for candidate in xpath_candidates:
        containers = tree.xpath(candidate['xpath'])
        print(f"\n[{candidate['name']}]")
        print(f"  XPath: {candidate['xpath']}")
        print(f"  Found: {len(containers)} containers")

        # 첫 3개 컨테이너에서 제품명 확인
        if containers:
            for i, container in enumerate(containers[:3], 1):
                # 제품명 추출 시도
                product_names = container.xpath('.//span[@data-automation-id="product-title"]')
                if product_names:
                    name = product_names[0].text_content().strip()[:50]
                    print(f"    [{i}] {name}...")
                else:
                    print(f"    [{i}] (제품명 없음)")

    print("\n" + "="*80)
    print("Analysis Complete")
    print("="*80)

if __name__ == "__main__":
    analyze_containers()
