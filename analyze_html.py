"""
BestBuy HTML 분석 스크립트
실제 섹션 구조와 SKU 컨테이너 XPath 찾기
"""
from lxml import html, etree

def analyze_html():
    html_file = "C:/Users/gomguard/Documents/퀵오일/삼성전자/samsung_dx/bby_pmt_page.html"

    print(f"[INFO] HTML 파일 읽기: {html_file}")
    with open(html_file, 'r', encoding='utf-8') as f:
        content = f.read()

    print(f"[OK] 파일 크기: {len(content):,} bytes\n")

    tree = html.fromstring(content)

    # 1. 모든 section 태그 찾기
    print("=" * 80)
    print("1. 모든 <section> 태그 분석")
    print("=" * 80)
    sections = tree.xpath('//section')
    print(f"총 {len(sections)}개 section 발견\n")

    for idx, section in enumerate(sections[:10], 1):  # 처음 10개만
        classes = section.get('class', '')
        print(f"Section {idx}: class=\"{classes}\"")
        # 섹션 내 h2, h3, span 텍스트 찾기
        headings = section.xpath('.//h2|.//h3|.//span[contains(@class, "headline")]')
        if headings:
            for h in headings[:3]:
                text = h.text_content().strip()[:80]
                if text:
                    print(f"  - {h.tag}: {text}")
        print()

    # 2. hero-holiday-blue-gradient 섹션 찾기
    print("=" * 80)
    print("2. hero-holiday-blue-gradient 섹션")
    print("=" * 80)
    hero_sections = tree.xpath('//section[contains(@class, "hero-holiday-blue-gradient")]')
    print(f"총 {len(hero_sections)}개 발견\n")

    for idx, section in enumerate(hero_sections, 1):
        print(f"Hero Section {idx}:")
        # promotion_type 찾기
        spans = section.xpath('.//span')
        for span in spans[:5]:
            text = span.text_content().strip()
            if text and len(text) > 5:
                classes = span.get('class', '')
                print(f"  span[class=\"{classes[:60]}\"]: {text[:80]}")
        print()

    # 3. c-carousel-list 찾기
    print("=" * 80)
    print("3. c-carousel-list (SKU 컨테이너)")
    print("=" * 80)
    carousels = tree.xpath('//ul[@class="c-carousel-list"]')
    print(f"총 {len(carousels)}개 발견\n")

    for idx, carousel in enumerate(carousels[:3], 1):
        items = carousel.xpath('.//li')
        print(f"Carousel {idx}: {len(items)}개 li 아이템")
        # 부모 section 찾기
        parent = carousel.getparent()
        for _ in range(5):
            if parent is not None:
                if parent.tag == 'section':
                    classes = parent.get('class', '')
                    print(f"  → 부모 section: class=\"{classes}\"")
                    break
                parent = parent.getparent()
        print()

    # 4. pl-flex-carousel-container 찾기
    print("=" * 80)
    print("4. pl-flex-carousel-container (SKU 컨테이너)")
    print("=" * 80)
    flex_carousels = tree.xpath('//div[contains(@class, "pl-flex-carousel-container")]')
    print(f"총 {len(flex_carousels)}개 발견\n")

    for idx, container in enumerate(flex_carousels[:3], 1):
        # 내부 아이템 찾기
        items = container.xpath('.//li|.//div[contains(@class, "product")]|.//a')
        print(f"Flex Container {idx}: {len(items)}개 아이템")
        # 부모 section 찾기
        parent = container.getparent()
        for _ in range(5):
            if parent is not None:
                if parent.tag == 'section':
                    classes = parent.get('class', '')
                    print(f"  → 부모 section: class=\"{classes}\"")
                    break
                parent = parent.getparent()

        # 내부 구조 확인
        li_items = container.xpath('.//li')
        if li_items:
            print(f"  → li 태그: {len(li_items)}개")
            if li_items:
                li_classes = li_items[0].get('class', '')
                print(f"     첫 번째 li class: \"{li_classes[:80]}\"")
        print()

    # 5. 특정 텍스트 포함 섹션 찾기
    print("=" * 80)
    print("5. 'On-sale TVs' 텍스트 포함 섹션")
    print("=" * 80)
    sections_with_text = tree.xpath('//section[contains(., "On-sale TVs")]')
    print(f"총 {len(sections_with_text)}개 발견\n")

    for idx, section in enumerate(sections_with_text[:2], 1):
        classes = section.get('class', '')
        print(f"Section {idx}: class=\"{classes}\"")
        # 관련 SKU 컨테이너 찾기
        carousels = section.xpath('.//ul[@class="c-carousel-list"]|.//div[contains(@class, "pl-flex-carousel")]')
        print(f"  → SKU 컨테이너: {len(carousels)}개")
        for c_idx, carousel in enumerate(carousels[:2], 1):
            tag = carousel.tag
            c_class = carousel.get('class', '')[:60]
            items = carousel.xpath('.//li')
            print(f"     {c_idx}. <{tag} class=\"{c_class}\">: {len(items)}개 li")
        print()

if __name__ == "__main__":
    analyze_html()
