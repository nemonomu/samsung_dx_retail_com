"""
SEA TV Amazon Detail Review Test (260429)

amazon_tv_dt1.AmazonDetailCrawler의 새 추출 경로 검증:
  - _lazy_load_reviews() (scroll.to_bottom + cardCount 안정화)
  - extract_summarized_review(tree) (DB priority list)
  - extract_detailed_reviews(tree) (DB priority list, lxml 기반)

로그에는 리뷰요약본(summarized) + 리뷰본문(detailed) 두 가지만 출력.
"""

import time
import random
import traceback
from lxml import html

from amazon_tv_dt1 import AmazonDetailCrawler


TEST_URLS = [
    "https://www.amazon.com/Samsung-Electronics-UN32M4500A-32-Inch-Renewed/dp/B07J1WTL8V/ref=sr_1_282?dib=eyJ2IjoiMSJ9.mIfWaZflk8odfpUROMPy3ZeBNrqa_rdE6Dy71BrgnoZ421XS_F0mPh62ireFK4jom3XvUAcojncDvxuZ5eL-Lu1u1Y7JLbhp94lRYcF0R5f5GJoMvWbtuUTH6puqDi5mjJtVwbrnyZ5a0kfeeBN533yQl-3Rp7zX--mSxNPFDZeXEusVzMoiZl8L5GXiDhgGPfjC0-BpV3L15jkY7vUqmcehWHqneSNkudbnl0urzAI.OTWZGZTIwHU29AFtiCjp6clBjifwL6xzW-VwLgVxYk0&dib_tag=se&keywords=tv&qid=1777392372&sr=8-282&xpid=khdVh5qeglJy2&th=1",
    "https://www.amazon.com/TCL-55S425-inch-Smart-Roku/dp/B07JJZPZNS/ref=sr_1_117?dib=eyJ2IjoiMSJ9.hmUv17qrgZ6S9Ng1LSRDOe9jPQLznx2w4z7fK5CCUtYYyJA3JmI3KSczuJbFOK-NUou2VlijROOXs9WUxw3T8-3OUVqTFN_Jm7a5Ae5QLOi7cO9Qc2Mbs2laMiiJTd7D_6JYqhD9gH5rNuC0OBUkSGqYsmiZh9R7DApDwsvToe6ICrCn3krzP4XqrQ2pXIM318cJQ3x2KERRDar5Zxr6eTjrZ3_UWgf04n9uzl2pilE.aBVjgPvJTNptBQLQtCbWpRh4DFjcfFPC2jE4A4m8p5Q&dib_tag=se&keywords=tv&qid=1776960169&sr=8-117&xpid=khdVh5qeglJy2&th=1",
    "https://www.amazon.com/VIZIO-Class-49-5-Diag-Smart/dp/B07KJB92L9/ref=sr_1_291?dib=eyJ2IjoiMSJ9.ZDpEf97BDrt1slTzON_EUt45bn2QxWg6WH-yjv4hPCVmMQzhiZY9XTlbjKp7fbJb8wk_S682-sVsT75E1mX3D0skbpaSPw4zmBfjuxvB-KNb380cBGXKOcmyHcKzFJBK67qM7Gaa-YF6zCW0DA0GZlYwCZqZGMxBBfmFmZ5fcN0gIUD_Q5tWvmdxXlCYIDuZyJdQBYel47dH6QwRObVLY47rkLZabEonTIyAJjF5ZJo.UxBo7Fn5ikjyIlu7EVY_LSVRVBO8QkyfFP745Bq--8A&dib_tag=se&keywords=tv&qid=1776960392&sr=8-291&xpid=khdVh5qeglJy2",
    "https://www.amazon.com/Tyler-TTV705-14-Portable-Battery-Television/dp/B07MC9KHK3/ref=sr_1_259?dib=eyJ2IjoiMSJ9.oAbyxpl5dP_4RJCAQbLQEPF7v2_gyedV-Osr2pvmkz08geN6gOU67muvfCZC483lHLhucwPNH_4GOOC50O3iUis3Lv0iow5s20_kx8J3hO15m6_kAgIvOpgcoEarKjsCDQSy5iO6HCJwqIPiE_T3DgvuIFq4ORNs4rNL_csaBHFGmzuH34KK0Z02sRj041ff0a-KtewWpKczBNuESOmbLJngXnLH4qeekcS4l_SIUhg.X6lhDGAdqyTyuix6gTv_7EKn-Hac4YRbNfBptMxrU0U&dib_tag=se&keywords=tv&qid=1777262764&sbo=RZvfv%2F%2FHxDF%2BO5021pAnSA%3D%3D&sr=8-259&xpid=khdVh5qeglJy2",
]


def test_one(crawler, idx, total, url):
    print()
    print('=' * 80)
    print(f'[TEST {idx}/{total}] {url[:100]}...')
    print('=' * 80)

    try:
        crawler.page.get(url)
        time.sleep(random.uniform(3, 5))

        # Sorry/Robot/Captcha 간이 체크 (실패하면 그냥 진행 - 결과로 판단)
        try:
            if hasattr(crawler, 'check_and_handle_sorry_page'):
                crawler.check_and_handle_sorry_page(max_retries=2)
        except Exception:
            pass

        # 새 경로 그대로 호출
        crawler._lazy_load_reviews()
        tree = html.fromstring(crawler.page.html)

        summary = crawler.extract_summarized_review(tree)
        detailed = crawler.extract_detailed_reviews(tree)

        print()
        print('--- Summarized_Review_Content ---')
        print(summary if summary else '(NULL)')

        print()
        print('--- Detailed_Review_Content ---')
        if detailed:
            count = len(detailed.split(' ||| '))
            print(f'(extracted {count} reviews)')
            print(detailed)
        else:
            print('(NULL)')

    except Exception as e:
        print(f'[ERROR] {e}')
        traceback.print_exc()


def main():
    crawler = AmazonDetailCrawler()
    if not crawler.connect_db():
        return
    if not crawler.load_xpaths():
        return
    crawler.setup_driver()

    try:
        for idx, url in enumerate(TEST_URLS, 1):
            test_one(crawler, idx, len(TEST_URLS), url)
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


if __name__ == '__main__':
    main()
