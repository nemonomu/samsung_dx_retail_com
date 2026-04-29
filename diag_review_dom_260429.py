"""
Quick diagnostic — open a single Amazon URL and report which review DOM pattern is used.

Usage:
  python diag_review_dom_260429.py [URL]

기본 URL: B001U3Y8MM (cards=0 케이스 진단용).
"""

import sys
import time
import json

from DrissionPage import ChromiumPage, ChromiumOptions


DEFAULT_URL = (
    "https://www.amazon.com/Samsung-LN22B460-26-Inch-720p-HDTV/dp/B001U3Y8MM/"
    "ref=sr_1_287?dib=eyJ2IjoiMSJ9.mIfWaZflk8odfpUROMPy3ZeBNrqa_rdE6Dy71BrgnoZ421XS_F0mPh62ireFK4jom3XvUAcojncDvxuZ5eL-Lu1u1Y7JLbhp94lRYcF0R5f5GJoMvWbtuUTH6puqDi5mjJtVwbrnyZ5a0kfeeBN533yQl-3Rp7zX--mSxNPFDZeXEusVzMoiZl8L5GXiDhgGPfjC0-BpV3L15jkY7vUqmcehWHqneSNkudbnl0urzAI.OTWZGZTIwHU29AFtiCjp6clBjifwL6xzW-VwLgVxYk0&dib_tag=se&keywords=tv&qid=1777392372&sr=8-287&xpid=khdVh5qeglJy2"
)


def main():
    url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_URL
    print(f'[INFO] URL: {url[:120]}')

    co = ChromiumOptions()
    co.set_argument('--lang=en-US')
    page = ChromiumPage(co)
    page.set.window.max()

    try:
        page.get(url)
        time.sleep(5)
        print(f'[INFO] page title: {page.title}')

        page.scroll.to_bottom()
        time.sleep(8)

        js = '''
        function safeText(el) { return el ? (el.innerText || '').slice(0, 100) : null; }
        var allReviewIds = Array.from(document.querySelectorAll('[id]'))
            .map(function(el) { return el.id; })
            .filter(function(id) { return id.toLowerCase().indexOf('review') !== -1; })
            .slice(0, 30);
        var dataHookValues = Array.from(document.querySelectorAll('[data-hook]'))
            .map(function(el) { return el.getAttribute('data-hook'); })
            .filter(function(h) { return h && h.toLowerCase().indexOf('review') !== -1; });
        var dataHookCounts = {};
        dataHookValues.forEach(function(h) { dataHookCounts[h] = (dataHookCounts[h] || 0) + 1; });

        // sample one review-body to see body text path
        var sampleBody = document.querySelector('[data-hook="review-body"]') ||
                         document.querySelector('[data-hook="review-collapsed"]');
        var sampleBodyHTML = sampleBody ? sampleBody.outerHTML.slice(0, 400) : null;

        return {
            page_title: document.title,
            url: location.href,
            count_customer_review: document.querySelectorAll('[id^="customer_review-"]').length,
            count_customer_review_foreign: document.querySelectorAll('[id^="customer_review_foreign-"]').length,
            count_data_hook_review: document.querySelectorAll('[data-hook="review"]').length,
            count_data_hook_review_body: document.querySelectorAll('[data-hook="review-body"]').length,
            count_data_hook_review_collapsed: document.querySelectorAll('[data-hook="review-collapsed"]').length,
            has_reviewsMedley: !!document.getElementById('reviewsMedley'),
            has_cm_cr_dp_review_list: !!document.getElementById('cm-cr-dp-review-list'),
            has_cr_product_insights: document.querySelectorAll('[id^="cr-product"]').length,
            iframes: document.querySelectorAll('iframe').length,
            sample_id_with_review: allReviewIds,
            data_hook_review_counts: dataHookCounts,
            sample_review_body_html: sampleBodyHTML
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
