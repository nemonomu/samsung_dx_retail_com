"""
count_of_reviews Priority 0 테스트 (seeAllReviewsStarRating 링크 기반)
- 기존 "Showing X of Y" / "View all reviews (N)" 가 놓치던 단순 "N reviews" 형식 검증
- 각 URL 별 count_of_reviews 결과 로그
- 총 소요시간 마지막에 출력
- DB 저장 없음
"""
import time
import re
import random
from DrissionPage import ChromiumPage, ChromiumOptions
from lxml import html


TEST_URLS = [
    "https://www.walmart.com/ip/Samsung-QN85QN90F-85-inch-Class-QN90F-Series-Neo-QLED-4K-Vision-AI-Mini-LED-Smart-TV/15943813649",
    "https://www.walmart.com/ip/SAMSUNG-77-Class-S85D-OLED-Smart-TV-QN77S85DAEXZA-2024/5347546898",
    "https://www.walmart.com/ip/65-Class-The-Frame-LS03FAF-Series-QLED-4K-HDR-Samsung-Vision-AI-Smart-TV/7847054238",
    "https://www.walmart.com/ip/55-Class-The-Frame-LS03FAF-Series-QLED-4K-HDR-Samsung-Vision-AI-Smart-TV/16003713987",
    "https://www.walmart.com/ip/Samsung-The-Frame-Pro-LS03FW-Neo-QLED-4K-65-Inch-Smart-TV-in-Black-QN65LS03FWFXZA/16114970727",
    "https://www.walmart.com/ip/Hisense-85-inch-Class-S7-4K-QLED-Smart-CanvasTV-Google-TV/16502513727",
    "https://www.walmart.com/ip/Element-40-1080p-HD-Xumo-TV-E300AE40C/18711419608",
    "https://www.walmart.com/ip/Samsung-65-UHD-TV/18200354479",
    "https://www.walmart.com/ip/Samsung-85-UHD-TV/18142774244",
]


def extract_count_of_reviews_priority0(tree):
    """wmart_tv_dt1.py 에 추가된 Priority 0 로직과 동일
    Returns: (count, raw_text)
        count: int or None
        raw_text: element 의 텍스트 (없으면 None)
    """
    try:
        elements = tree.xpath('//a[@link-identifier="seeAllReviewsStarRating"]')
        if not elements:
            return None, None
        text = elements[0].text_content().strip() if hasattr(elements[0], 'text_content') else str(elements[0]).strip()
        match = re.match(r'^([\d,]+)\s+reviews?', text, re.IGNORECASE)
        if match:
            return int(match.group(1).replace(',', '')), text
        return None, text
    except Exception as e:
        return None, f"ERROR: {e}"


def main():
    print("=" * 80)
    print("  count_of_reviews Priority 0 Test")
    print(f"  URL 수: {len(TEST_URLS)}")
    print("=" * 80)

    # HHP 스타일 config (wmart_tv_dt1.py 동일)
    co = ChromiumOptions()
    co.set_pref('profile.managed_default_content_settings.images', 1)
    co.set_pref('profile.managed_default_content_settings.media_stream', 2)
    co.set_pref('profile.managed_default_content_settings.plugins', 2)
    co.set_argument('--autoplay-policy=document-user-activation-required')
    page = ChromiumPage(co)
    print("[OK] Browser ready (images enabled, media/plugins disabled, autoplay blocked)")

    # 세션 웜업
    print("[INFO] 홈페이지 접속 (세션 웜업)...")
    page.get("https://www.walmart.com")
    time.sleep(random.uniform(5, 8))

    overall_start = time.time()
    results = []

    for idx, url in enumerate(TEST_URLS, 1):
        url_start = time.time()
        print(f"\n{'='*80}")
        print(f"[{idx}/{len(TEST_URLS)}] {url}")
        print(f"{'='*80}")
        try:
            page.get(url)
            try:
                page.ele('xpath://h1', timeout=10)
                print("  [OK] h1 loaded")
            except Exception:
                print("  [WARN] h1 timeout, 계속 진행")
            time.sleep(random.uniform(2, 3))

            page_source = page.run_js('return document.documentElement.outerHTML', timeout=30)
            tree = html.fromstring(page_source)

            count, raw_text = extract_count_of_reviews_priority0(tree)
            elapsed = time.time() - url_start

            if count is not None:
                print(f"  [OK] count_of_reviews = {count}")
                print(f"       raw text: {raw_text!r}")
                status = f"SUCCESS count={count}"
            elif raw_text:
                print(f"  [WARN] element 있지만 regex 매칭 실패")
                print(f"         raw text: {raw_text!r}")
                status = f"REGEX_FAIL text={raw_text!r}"
            else:
                print(f"  [INFO] link-identifier='seeAllReviewsStarRating' element 없음")
                status = "NO_ELEMENT"
            print(f"  [TIME] URL 처리: {elapsed:.1f}초")
            results.append({'url': url, 'count': count, 'status': status, 'elapsed': elapsed})

        except Exception as e:
            elapsed = time.time() - url_start
            print(f"  [ERROR] {e}")
            print(f"  [TIME] URL 처리: {elapsed:.1f}초")
            results.append({'url': url, 'count': None, 'status': f'ERROR: {e}', 'elapsed': elapsed})

        time.sleep(random.uniform(3, 5))

    overall_elapsed = time.time() - overall_start

    # 최종 요약
    print("\n" + "=" * 80)
    print("  요약")
    print("=" * 80)
    success_count = sum(1 for r in results if r['count'] is not None)
    print(f"  성공: {success_count}/{len(results)}")
    print()
    for r in results:
        url_short = r['url'][-70:] if len(r['url']) > 70 else r['url']
        count_str = str(r['count']) if r['count'] is not None else '-'
        print(f"  count={count_str:>6s}  {r['elapsed']:5.1f}s  {url_short}")

    # 총 수집 시간
    print("\n" + "=" * 80)
    print(f"  [TOTAL] 총 수집 시간: {overall_elapsed:.1f}초 ({overall_elapsed/60:.1f}분)")
    print(f"  [TOTAL] URL당 평균: {overall_elapsed/len(TEST_URLS):.1f}초")
    print("=" * 80)

    try:
        page.quit()
    except Exception:
        pass
    print("[DONE]")


if __name__ == "__main__":
    main()
