"""
Quick diagnostic — open a single Amazon URL and report which review DOM pattern is used.

Usage:
  python diag_review_dom_260429.py [URL] [--cookies <pkl_path>]

기본 URL: B001U3Y8MM (cards=0 케이스 진단용).
--cookies 가 주어지면 해당 .pkl 쿠키로 로그인 상태 진단.
"""

import sys
import time
import json
import pickle
import os

from DrissionPage import ChromiumPage, ChromiumOptions


DEFAULT_URL = (
    "https://www.amazon.com/Samsung-LN22B460-26-Inch-720p-HDTV/dp/B001U3Y8MM/"
    "ref=sr_1_287?dib=eyJ2IjoiMSJ9.mIfWaZflk8odfpUROMPy3ZeBNrqa_rdE6Dy71BrgnoZ421XS_F0mPh62ireFK4jom3XvUAcojncDvxuZ5eL-Lu1u1Y7JLbhp94lRYcF0R5f5GJoMvWbtuUTH6puqDi5mjJtVwbrnyZ5a0kfeeBN533yQl-3Rp7zX--mSxNPFDZeXEusVzMoiZl8L5GXiDhgGPfjC0-BpV3L15jkY7vUqmcehWHqneSNkudbnl0urzAI.OTWZGZTIwHU29AFtiCjp6clBjifwL6xzW-VwLgVxYk0&dib_tag=se&keywords=tv&qid=1777392372&sr=8-287&xpid=khdVh5qeglJy2"
)


def parse_args():
    url = DEFAULT_URL
    cookies_path = None
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        a = args[i]
        if a == '--cookies' and i + 1 < len(args):
            cookies_path = args[i + 1]
            i += 2
        elif a.startswith('--cookies='):
            cookies_path = a.split('=', 1)[1]
            i += 1
        else:
            url = a
            i += 1
    return url, cookies_path


def load_cookies(page, cookies_path):
    """홈페이지 접속 후 기존 쿠키 클리어, pkl 로드, refresh."""
    print(f'[INFO] Loading cookies from {cookies_path}')
    if not os.path.exists(cookies_path):
        print(f'[ERROR] cookie file not found: {cookies_path}')
        sys.exit(1)

    page.get('https://www.amazon.com')
    time.sleep(2)
    try:
        page.set.cookies.clear()
    except Exception:
        pass

    with open(cookies_path, 'rb') as f:
        cookies = pickle.load(f)
    loaded = 0
    for cookie in cookies:
        try:
            page.set.cookies(cookie)
            loaded += 1
        except Exception:
            pass
    print(f'[INFO] loaded {loaded}/{len(cookies)} cookies')
    page.refresh()
    time.sleep(3)


def main():
    url, cookies_path = parse_args()
    print(f'[INFO] URL: {url[:120]}')
    print(f'[INFO] mode: {"LOGGED-IN (" + cookies_path + ")" if cookies_path else "ANONYMOUS"}')

    co = ChromiumOptions()
    co.set_argument('--lang=en-US')
    page = ChromiumPage(co)
    page.set.window.max()

    try:
        if cookies_path:
            load_cookies(page, cookies_path)

        page.get(url)
        time.sleep(5)
        print(f'[INFO] page title: {page.title}')

        page.scroll.to_bottom()
        time.sleep(8)

        js = '''
        var allReviewIds = Array.from(document.querySelectorAll('[id]'))
            .map(function(el) { return el.id; })
            .filter(function(id) { return id.toLowerCase().indexOf('review') !== -1; })
            .slice(0, 30);
        var dataHookValues = Array.from(document.querySelectorAll('[data-hook]'))
            .map(function(el) { return el.getAttribute('data-hook'); })
            .filter(function(h) { return h && h.toLowerCase().indexOf('review') !== -1; });
        var dataHookCounts = {};
        dataHookValues.forEach(function(h) { dataHookCounts[h] = (dataHookCounts[h] || 0) + 1; });

        var sampleBody = document.querySelector('[data-hook="review-body"]') ||
                         document.querySelector('[data-hook="review-collapsed"]') ||
                         document.querySelector('[data-hook="reviewText"]');
        // 전체 outerHTML — XPath 결정용 (잘리지 않게 length 표기 + 일부 dump)
        var sampleBodyHTML = sampleBody ? sampleBody.outerHTML : null;
        var sampleBodyTextRaw = sampleBody ? (sampleBody.innerText || '').slice(0, 600) : null;

        // first review container outerHTML preview (modern OR legacy)
        var firstContainer = document.querySelector('[id^="customer_review-"]') ||
                             document.querySelector('[id^="customer_review_foreign-"]') ||
                             document.querySelector('[data-hook="review"]');
        var sampleContainerHTML = firstContainer ? firstContainer.outerHTML.slice(0, 600) : null;

        // XPath candidates — 어느 path가 teaser 없이 본문만 추출하는지 비교
        function evalXPath(xpath) {
            var snap = document.evaluate(xpath, document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
            var out = [];
            for (var i = 0; i < snap.snapshotLength && i < 3; i++) {
                var n = snap.snapshotItem(i);
                var t = (n.innerText || n.textContent || '').replace(/\\s+/g, ' ').trim().slice(0, 200);
                out.push(t);
            }
            return {count: snap.snapshotLength, samples: out};
        }
        var xpathTests = {
            'A1_current_reviewText':
                evalXPath('//div[@id="reviewsMedley"]//*[@data-hook="reviewText"]'),
            'A2_card_content_span':
                evalXPath('//div[@id="reviewsMedley"]//*[@data-hook="reviewText"]//span[contains(@class, "cardui-deck-card-content")]'),
            'A3_expand_div':
                evalXPath('//div[@id="reviewsMedley"]//*[@data-hook="reviewText"]/div[contains(@class, "a-teaser-describedby-expand")]'),
            'A4_not_collapsed':
                evalXPath('//div[@id="reviewsMedley"]//*[@data-hook="reviewText"]/div[not(contains(@class, "a-teaser-describedby-collapsed")) and not(contains(@class, "a-expander-prompt"))]'),
            'A5_a_cardui_content':
                evalXPath('//div[@id="reviewsMedley"]//*[@data-hook="reviewText"]//div[contains(@class, "a-cardui-content")]'),
            'A6_visible_text_only':
                evalXPath('//div[@id="reviewsMedley"]//*[@data-hook="reviewText"]//span[not(ancestor::*[contains(@class, "a-hidden")]) and not(contains(@class, "a-expander-prompt-text")) and not(contains(@class, "describedby"))]'),
        };

        // is logged in? check for sign-in element
        var signInEl = document.querySelector('#nav-link-accountList-nav-line-1');
        var signedIn = signInEl ? !signInEl.innerText.toLowerCase().includes('sign in') : null;

        return {
            page_title: document.title,
            url: location.href,
            signed_in_hint: signedIn,
            count_customer_review: document.querySelectorAll('[id^="customer_review-"]').length,
            count_customer_review_foreign: document.querySelectorAll('[id^="customer_review_foreign-"]').length,
            count_data_hook_review: document.querySelectorAll('[data-hook="review"]').length,
            count_data_hook_review_body: document.querySelectorAll('[data-hook="review-body"]').length,
            count_data_hook_review_collapsed: document.querySelectorAll('[data-hook="review-collapsed"]').length,
            count_data_hook_review_text: document.querySelectorAll('[data-hook="reviewText"]').length,
            has_reviewsMedley: !!document.getElementById('reviewsMedley'),
            has_cm_cr_dp_review_list: !!document.getElementById('cm-cr-dp-review-list'),
            has_cr_product_insights: document.querySelectorAll('[id^="cr-product"]').length,
            iframes: document.querySelectorAll('iframe').length,
            sample_id_with_review: allReviewIds,
            data_hook_review_counts: dataHookCounts,
            sample_review_body_html_len: sampleBodyHTML ? sampleBodyHTML.length : 0,
            sample_review_body_html: sampleBodyHTML,
            sample_review_body_innertext: sampleBodyTextRaw,
            sample_container_html: sampleContainerHTML,
            xpath_tests: xpathTests
        };
        '''
        result = page.run_js(js)
        print()
        print('=== DOM DIAG ===')
        print(json.dumps(result, indent=2, ensure_ascii=False))

    finally:
        try:
            page.quit()
        except Exception:
            pass


if __name__ == '__main__':
    main()
