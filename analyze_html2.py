"""
섹션과 SKU 컨테이너의 연결 구조 분석
"""
from lxml import html

def analyze_section_structure():
    html_file = "bby_pmt_page.html"

    print(f"[INFO] HTML 파일 읽기: {html_file}")
    with open(html_file, 'r', encoding='utf-8') as f:
        content = f.read()

    tree = html.fromstring(content)

    # 각 섹션과 그 다음 형제/부모 요소 분석
    print("=" * 80)
    print("섹션별 상세 구조 분석")
    print("=" * 80)

    # Section 1: Don't-miss deals
    print("\n[Section 1] Don't-miss deals on TVs")
    print("-" * 80)
    section1 = tree.xpath('//section[contains(., "Don\'t-miss deals")]')
    if section1:
        section1 = section1[0]
        print(f"Section class: \"{section1.get('class', '')}\"")

        # 부모 확인
        parent = section1.getparent()
        if parent is not None:
            print(f"부모 태그: <{parent.tag}> class=\"{parent.get('class', '')[:60]}\"")

            # 부모 내의 모든 자식 확인
            siblings = parent.getchildren()
            section_idx = siblings.index(section1)
            print(f"부모의 {len(siblings)}개 자식 중 {section_idx + 1}번째")

            # 다음 형제 요소들 확인
            print("\n다음 형제 요소들:")
            for i in range(section_idx + 1, min(section_idx + 4, len(siblings))):
                sibling = siblings[i]
                tag = sibling.tag
                classes = sibling.get('class', '')[:60]
                print(f"  {i - section_idx}. <{tag}> class=\"{classes}\"")

                # SKU 컨테이너 찾기
                carousels = sibling.xpath('.//ul[@class="c-carousel-list"]|.//div[contains(@class, "pl-flex-carousel")]')
                if carousels:
                    print(f"     → SKU 컨테이너 {len(carousels)}개 발견!")
                    for c in carousels[:1]:
                        items = c.xpath('.//li[@class="item c-carousel-item "]')
                        print(f"        - {len(items)}개 li 아이템")

    # Section 2: On-sale TVs
    print("\n" + "=" * 80)
    print("[Section 2] On-sale TVs as low as $69.99")
    print("-" * 80)
    section2 = tree.xpath('//section[contains(@class, "hero-holiday-blue-gradient")][contains(., "On-sale TVs")]')
    if section2:
        section2 = section2[0]
        print(f"Section class: \"{section2.get('class', '')}\"")

        # 부모 확인
        parent = section2.getparent()
        if parent is not None:
            print(f"부모 태그: <{parent.tag}> class=\"{parent.get('class', '')[:60]}\"")

            # 부모 내의 모든 자식 확인
            siblings = parent.getchildren()
            section_idx = siblings.index(section2)
            print(f"부모의 {len(siblings)}개 자식 중 {section_idx + 1}번째")

            # 다음 형제 요소들 확인
            print("\n다음 형제 요소들:")
            for i in range(section_idx + 1, min(section_idx + 4, len(siblings))):
                sibling = siblings[i]
                tag = sibling.tag
                classes = sibling.get('class', '')[:60]
                print(f"  {i - section_idx}. <{tag}> class=\"{classes}\"")

                # SKU 컨테이너 찾기
                carousels = sibling.xpath('.//ul[@class="c-carousel-list"]|.//div[contains(@class, "pl-flex-carousel")]')
                if carousels:
                    print(f"     → SKU 컨테이너 {len(carousels)}개 발견!")
                    for c in carousels[:1]:
                        items = c.xpath('.//li[@class="item c-carousel-item "]')
                        print(f"        - {len(items)}개 li 아이템")

    # Section 3: Save on select gaming
    print("\n" + "=" * 80)
    print("[Section 3] Save on select gaming TVs")
    print("-" * 80)
    section3 = tree.xpath('//section[contains(@class, "hero-holiday-blue-gradient")][contains(., "gaming TVs")]')
    if section3:
        section3 = section3[0]
        print(f"Section class: \"{section3.get('class', '')}\"")

        # 부모 확인
        parent = section3.getparent()
        if parent is not None:
            print(f"부모 태그: <{parent.tag}> class=\"{parent.get('class', '')[:60]}\"")

            # 부모 내의 모든 자식 확인
            siblings = parent.getchildren()
            section_idx = siblings.index(section3)
            print(f"부모의 {len(siblings)}개 자식 중 {section_idx + 1}번째")

            # 다음 형제 요소들 확인
            print("\n다음 형제 요소들:")
            for i in range(section_idx + 1, min(section_idx + 4, len(siblings))):
                sibling = siblings[i]
                tag = sibling.tag
                classes = sibling.get('class', '')[:60]
                print(f"  {i - section_idx}. <{tag}> class=\"{classes}\"")

                # SKU 컨테이너 찾기
                carousels = sibling.xpath('.//ul[@class="c-carousel-list"]|.//div[contains(@class, "pl-flex-carousel")]')
                if carousels:
                    print(f"     → SKU 컨테이너 {len(carousels)}개 발견!")
                    for c in carousels[:1]:
                        items = c.xpath('.//li[@class="item c-carousel-item "]')
                        print(f"        - {len(items)}개 li 아이템")

    # 전체 구조에서 섹션과 가장 가까운 SKU 컨테이너 찾기
    print("\n" + "=" * 80)
    print("대안 방법: 섹션 다음의 SKU 컨테이너 (following-sibling)")
    print("=" * 80)

    sections_info = [
        ("Don't-miss deals", '//section[contains(., "Don\'t-miss deals")]'),
        ("On-sale TVs", '//section[contains(@class, "hero-holiday-blue-gradient")][contains(., "On-sale TVs")]'),
        ("gaming TVs", '//section[contains(@class, "hero-holiday-blue-gradient")][contains(., "gaming TVs")]')
    ]

    for name, xpath in sections_info:
        print(f"\n[{name}]")
        sections = tree.xpath(xpath)
        if sections:
            section = sections[0]

            # following-sibling으로 다음 요소들 찾기
            following = section.xpath('./following-sibling::*[position() <= 3]')
            print(f"다음 형제 요소: {len(following)}개")
            for f in following:
                tag = f.tag
                classes = f.get('class', '')[:60]
                print(f"  <{tag}> class=\"{classes}\"")

                # 내부에 SKU 컨테이너 있는지 확인
                carousels = f.xpath('.//ul[@class="c-carousel-list"]')
                if carousels:
                    items = carousels[0].xpath('.//li[@class="item c-carousel-item "]')
                    print(f"    → c-carousel-list 발견! {len(items)}개 li")

if __name__ == "__main__":
    analyze_section_structure()
