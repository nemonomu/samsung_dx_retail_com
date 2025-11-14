"""
DOM 순서 기반 섹션-SKU 컨테이너 매핑 분석
"""
from lxml import html

def analyze_dom_order():
    html_file = "bby_pmt_page.html"

    print(f"[INFO] HTML 파일 읽기: {html_file}")
    with open(html_file, 'r', encoding='utf-8') as f:
        content = f.read()

    tree = html.fromstring(content)

    print("=" * 80)
    print("1. 3개 섹션 찾기 (정확한 키워드)")
    print("=" * 80)

    section_keywords = [
        ("Section 1", "Don't-miss deals on TVs"),
        ("Section 1 (alt)", "Big savings for a limited time"),
        ("Section 2", "On-sale TVs"),
        ("Section 2 (alt)", "as low as $69"),
        ("Section 3", "Save on select"),
        ("Section 3 (alt)", "gaming TVs")
    ]

    for name, keyword in section_keywords:
        sections = tree.xpath(f'//section[contains(., "{keyword}")]')
        print(f"{name} ('{keyword}'): {len(sections)}개 발견")

    print("\n" + "=" * 80)
    print("2. 모든 c-carousel-list 위치 확인 (DOM 순서)")
    print("=" * 80)

    carousels = tree.xpath('//ul[@class="c-carousel-list"]')
    print(f"총 {len(carousels)}개 발견\n")

    for idx, carousel in enumerate(carousels, 1):
        items = carousel.xpath('.//li[@class="item c-carousel-item "]')
        print(f"Carousel {idx}: {len(items)}개 li 아이템")

        # 앞에 있는 가장 가까운 section 찾기 (preceding)
        preceding_sections = carousel.xpath('preceding::section')
        if preceding_sections:
            # 가장 마지막(가장 가까운) section
            nearest_section = preceding_sections[-1]
            section_text = nearest_section.text_content().strip()[:80]
            section_class = nearest_section.get('class', '')
            print(f"  → 앞의 가장 가까운 section: class=\"{section_class}\"")
            print(f"     텍스트: {section_text}...")
        print()

    print("=" * 80)
    print("3. 모든 pl-flex-carousel-container 위치 확인")
    print("=" * 80)

    flex_containers = tree.xpath('//div[contains(@class, "pl-flex-carousel-container")]')
    print(f"총 {len(flex_containers)}개 발견\n")

    for idx, container in enumerate(flex_containers, 1):
        items = container.xpath('.//li[@class="item c-carousel-item "]')
        print(f"Flex Container {idx}: {len(items)}개 li 아이템")

        # 앞에 있는 가장 가까운 section 찾기
        preceding_sections = container.xpath('preceding::section')
        if preceding_sections:
            nearest_section = preceding_sections[-1]
            section_text = nearest_section.text_content().strip()[:80]
            section_class = nearest_section.get('class', '')
            print(f"  → 앞의 가장 가까운 section: class=\"{section_class}\"")
            print(f"     텍스트: {section_text}...")
        print()

    print("=" * 80)
    print("4. 각 섹션 키워드별로 following 요소에서 SKU 컨테이너 찾기")
    print("=" * 80)

    # 정확한 키워드로 섹션 찾기
    test_keywords = [
        "Big savings for a limited time",
        "as low as",
        "gaming TVs"
    ]

    for keyword in test_keywords:
        print(f"\n[키워드: '{keyword}']")
        # 해당 키워드를 포함하는 요소 찾기
        elements = tree.xpath(f'//*[contains(text(), "{keyword}")]')
        print(f"'{keyword}' 포함 요소: {len(elements)}개")

        if elements:
            elem = elements[0]
            print(f"  요소: <{elem.tag}> class=\"{elem.get('class', '')[:60]}\"")

            # 이 요소의 부모 section 찾기
            parent = elem
            for _ in range(10):
                parent = parent.getparent()
                if parent is not None and parent.tag == 'section':
                    print(f"  → 부모 section: class=\"{parent.get('class', '')}\"")

                    # 이 section 이후에 나오는 첫 번째 c-carousel-list 찾기
                    following_carousels = tree.xpath(f'//section[contains(., "{keyword}")]/following::ul[@class="c-carousel-list"][1]')
                    if following_carousels:
                        carousel = following_carousels[0]
                        items = carousel.xpath('.//li[@class="item c-carousel-item "]')
                        print(f"  → 다음 c-carousel-list: {len(items)}개 li 아이템 ✓")
                    else:
                        print(f"  → 다음 c-carousel-list: 없음 ✗")
                    break

    print("\n" + "=" * 80)
    print("5. 전체 페이지 구조: body의 직계 자식 확인")
    print("=" * 80)

    body = tree.xpath('//body')
    if body:
        body = body[0]
        children = body.getchildren()
        print(f"body의 직계 자식: {len(children)}개\n")

        for idx, child in enumerate(children[:15], 1):
            tag = child.tag
            classes = child.get('class', '')[:60]
            text_preview = child.text_content().strip()[:50]
            print(f"{idx}. <{tag}> class=\"{classes}\"")

            # section이나 carousel 포함 여부
            has_section = len(child.xpath('.//section')) > 0
            has_carousel = len(child.xpath('.//ul[@class="c-carousel-list"]')) > 0

            if has_section or has_carousel:
                print(f"   섹션: {has_section}, 캐러셀: {has_carousel}")
                if text_preview:
                    print(f"   텍스트: {text_preview}...")
            print()

if __name__ == "__main__":
    analyze_dom_order()
