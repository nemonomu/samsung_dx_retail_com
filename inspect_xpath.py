"""
inspect.py — Amazon detail page xpath inspector

dt1 와 동일한 인증된 세션으로 페이지를 가져와서:
  1) 렌더된 HTML 을 파일로 저장 (수동 inspect 용)
  2) DB 의 xpath 그룹들을 실제 페이지에 적용한 결과를 출력
  3) 특정 그룹만 검사 (예: original_price)
  4) 인터랙티브 xpath 입력 모드

사용:
  python inspect_xpath.py <URL>
      → 모든 xpath 그룹 결과 + HTML 저장

  python inspect_xpath.py <URL> <group1> [group2 ...]
      → 지정한 그룹들만 검사 (예: delivery_primary delivery_secondary delivery_holiday)

  python inspect_xpath.py <URL> --interactive
      → 페이지 fetch 후 stdin 으로 xpath 받아 실시간 테스트

저장 위치:
  inspect_page.html    — 페이지 source
  inspect_result.txt   — xpath 결과 dump
"""

import sys
import os
import time
from lxml import html as lxml_html

from amazon_tv_dt1 import AmazonDetailCrawler


HTML_OUT = 'inspect_page.html'
RESULT_OUT = 'inspect_result.txt'


def short(s, n=120):
    if s is None:
        return '[None]'
    s = str(s).strip()
    if not s:
        return '[empty]'
    s = s.replace('\n', ' ').replace('\r', ' ')
    return s if len(s) <= n else s[:n] + '...'


def render_xpath_result(tree, xp):
    """xpath 한 개 적용 → (matched_count, [샘플 결과 문자열...])"""
    try:
        result = tree.xpath(xp)
    except Exception as e:
        return -1, [f'[XPATH ERROR] {e}']

    if not result:
        return 0, []

    samples = []
    for r in result[:5]:
        if isinstance(r, str):
            samples.append(short(r))
        else:
            try:
                samples.append(short(r.text_content()))
            except Exception:
                samples.append(short(str(r)))
    return len(result), samples


def test_group(tree, group_name, xpaths, out_lines):
    """xpath 그룹 전체를 페이지에 적용해서 결과 출력."""
    header = f"\n{'='*80}\n[{group_name}]  {len(xpaths)} xpaths\n{'='*80}"
    print(header)
    out_lines.append(header)
    if not xpaths:
        line = "  (no xpaths in DB)"
        print(line)
        out_lines.append(line)
        return

    for i, xp in enumerate(xpaths, 1):
        cnt, samples = render_xpath_result(tree, xp)
        line1 = f"  [{i}] {xp}"
        if cnt < 0:
            line2 = f"      ⤷ {samples[0]}"
        elif cnt == 0:
            line2 = f"      ⤷ (no match)"
        else:
            samples_str = '  |  '.join(samples)
            line2 = f"      ⤷ matched={cnt}  →  {samples_str}"
        print(line1)
        print(line2)
        out_lines.append(line1)
        out_lines.append(line2)


def interactive_loop(tree):
    print("\n" + "=" * 80)
    print("[INTERACTIVE] xpath 입력 (빈 줄 또는 'quit' 으로 종료)")
    print("=" * 80)
    while True:
        try:
            xp = input("xpath> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not xp or xp.lower() in ('quit', 'exit', 'q'):
            break
        cnt, samples = render_xpath_result(tree, xp)
        if cnt < 0:
            print(f"  → {samples[0]}")
        elif cnt == 0:
            print(f"  → (no match)")
        else:
            for s in samples:
                print(f"  → {s}")
            print(f"  total matched: {cnt}")


def inspect(url, xpath_groups=None, interactive=False):
    print(f"[URL] {url}")
    crawler = AmazonDetailCrawler()

    print("[STEP] connect_db ...")
    if not crawler.connect_db():
        print("[ERROR] connect_db failed")
        return
    print("[STEP] load_xpaths ...")
    if not crawler.load_xpaths():
        print("[ERROR] load_xpaths failed")
        return
    print("[STEP] setup_driver (cookie 로드 포함) ...")
    crawler.setup_driver()

    try:
        print(f"[FETCH] {url[:100]}...")
        crawler.page.get(url)
        time.sleep(5)

        # Item details 펼치기 (dt1 동일)
        try:
            btn = crawler.page.ele(
                'xpath://span[contains(text(), "Item details")]/ancestor::a[contains(@class, "a-expander-header")]',
                timeout=3,
            )
            if btn and btn.attr('aria-expanded') != 'true':
                btn.click(by_js=True)
                time.sleep(1)
                print("[INFO] Item details 펼침")
        except Exception:
            pass

        # lazy load reviews (dt1 동일)
        try:
            crawler._lazy_load_reviews()
        except Exception:
            pass

        page_source = crawler.page.html
        tree = lxml_html.fromstring(page_source)

        # 1) HTML 저장
        with open(HTML_OUT, 'w', encoding='utf-8') as f:
            f.write(page_source)
        print(f"\n[SAVED] HTML → {os.path.abspath(HTML_OUT)} ({len(page_source):,} bytes)")

        # 2) xpath 결과 dump
        out_lines = []
        out_lines.append(f"URL: {url}")
        out_lines.append(f"FETCHED: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        out_lines.append(f"HTML size: {len(page_source):,} bytes")

        if interactive:
            interactive_loop(tree)
        elif xpath_groups:
            # 다중 그룹 지원 — 사용자 지정 순서대로 검사
            for group_name in xpath_groups:
                xpaths = crawler.xpaths.get(group_name, [])
                test_group(tree, group_name, xpaths, out_lines)
        else:
            # 전체 그룹 — alphabetical
            for group_name in sorted(crawler.xpaths.keys()):
                test_group(tree, group_name, crawler.xpaths[group_name], out_lines)

        if not interactive:
            with open(RESULT_OUT, 'w', encoding='utf-8') as f:
                f.write('\n'.join(out_lines))
            print(f"\n[SAVED] xpath 결과 → {os.path.abspath(RESULT_OUT)}")

    finally:
        try:
            if crawler.page:
                crawler.page.quit()
        except Exception:
            pass
        try:
            if crawler.db_conn:
                crawler.db_conn.close()
        except Exception:
            pass


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    url = sys.argv[1]
    rest = sys.argv[2:]  # 그룹 이름들 (또는 --interactive)

    interactive = (len(rest) == 1 and rest[0] == '--interactive')
    xpath_groups = None if interactive else (rest or None)

    inspect(url, xpath_groups=xpath_groups, interactive=interactive)


if __name__ == '__main__':
    main()
