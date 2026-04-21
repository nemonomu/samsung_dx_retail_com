"""
tv_retail_com 누락 복구 스크립트 (wmart_tv_recover_main.py 의 후처리).

이 프로젝트의 Walmart_tv_detail_crawled / tv_retail_com 은 history 테이블이라
같은 product_url 에 대해 과거 여러 세션의 레코드가 누적되어 있다.
→ 특정 세션에 대한 복구는 "URL 단위 + crawl_datetime 윈도우" 로 판단해야 안전.

동작:
1. 진단 (SELECT only)
   - source = wmart_tv_main_1/2 (지정 batch_id) unique URL
   - tv_retail_com 에 session window 안 레코드가 있는 URL = 정상
   - tv_retail_com 에 session window 안 레코드가 없는 URL = 누락 대상
2. 누락 URL 을 두 그룹으로 분류:
   A. Walmart_tv_detail_crawled 의 window 안에 레코드 존재 → 그 row 복제로 INSERT
   B. Walmart_tv_detail_crawled 의 window 안에도 없음       → wmart_tv_recover_main.py 로 재크롤 필요
3. 사용자 y 확인 후 A 그룹만 tv_retail_com INSERT (crawl_datetime = target_datetime 으로 고정)

이전 버전에서 제공했던 out-of-window UPDATE 로직은 제거:
  history 테이블 특성상 "window 밖 레코드" = 과거 정상 세션 기록. UPDATE 하면 과거 기록이 손상됨.

crawl_datetime 은 varchar (c39fe07) → 문자열로 비교·바인딩.

사용:
    python wmart_tv_fix_recover_datetime.py <main1_batch_id> <main2_batch_id>
예:
    python wmart_tv_fix_recover_datetime.py 20260420_130014 20260420_130655
"""
import argparse
import sys
from datetime import datetime, timedelta

import psycopg2
import pytz

from config import DB_CONFIG


def _safe_int(v):
    if v is None or v == '':
        return None
    if isinstance(v, int):
        return v
    try:
        return int(str(v).replace(',', ''))
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('main1_batch_id')
    parser.add_argument('main2_batch_id')
    parser.add_argument(
        '--window-hours', type=float, default=8.0,
        help='target_datetime 기준 ±N시간을 세션 윈도우로 간주. (default: 8)'
    )
    args = parser.parse_args()

    kst = pytz.timezone('Asia/Seoul').localize(
        datetime.strptime(args.main1_batch_id, '%Y%m%d_%H%M%S')
    )
    target_dt = kst.astimezone().replace(tzinfo=None)
    window = timedelta(hours=args.window_hours)
    fmt = '%Y-%m-%d %H:%M:%S'
    target_str = target_dt.strftime(fmt)
    win_start_str = (target_dt - window).strftime(fmt)
    win_end_str = (target_dt + window).strftime(fmt)

    print(f"[INFO] main1_batch_id  = {args.main1_batch_id} (KST)")
    print(f"[INFO] target_datetime = {target_str} (local)")
    print(f"[INFO] session window  = [{win_start_str} ~ {win_end_str}]")
    print(f"[INFO] (crawl_datetime is varchar — string compare)")

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT DISTINCT Product_url FROM (
                SELECT Product_url FROM wmart_tv_main_1
                 WHERE batch_id = %s AND Product_url IS NOT NULL AND Product_url != ''
                UNION
                SELECT Product_url FROM wmart_tv_main_2
                 WHERE batch_id = %s AND Product_url IS NOT NULL AND Product_url != ''
            ) s
        """, (args.main1_batch_id, args.main2_batch_id))
        source_urls = [r[0] for r in cur.fetchall()]
        print(f"\n[INFO] source URLs (main1+main2 unique): {len(source_urls)}")
        if not source_urls:
            print("[ERROR] No source URLs found")
            return

        cur.execute("""
            SELECT DISTINCT product_url FROM tv_retail_com
             WHERE product_url = ANY(%s)
               AND crawl_datetime >= %s AND crawl_datetime <= %s
        """, (source_urls, win_start_str, win_end_str))
        retail_in_window = {r[0] for r in cur.fetchall()}
        retail_missing = [u for u in source_urls if u not in retail_in_window]
        print(f"[INFO] tv_retail_com in-window URLs     : {len(retail_in_window)}")
        print(f"[INFO] tv_retail_com missing (session)  : {len(retail_missing)}")

        if not retail_missing:
            print("\n[OK] tv_retail_com already has all source URLs in this session. Nothing to do.")
            return

        cur.execute("""
            SELECT DISTINCT ON (product_url)
                   product_url, page_type, Retailer_SKU_Name, item, Star_Rating,
                   Number_of_ppl_purchased_yesterday, Number_of_ppl_added_to_carts,
                   SKU_Popularity, Savings, Discount_Type, Shipping_Info,
                   Count_of_Star_Ratings, Retailer_SKU_Name_similar, Detailed_Review_Content,
                   calendar_week,
                   final_sku_price, original_sku_price, pick_up_availability,
                   shipping_availability, delivery_availability, sku_status,
                   retailer_membership_discounts, available_quantity_for_purchase,
                   inventory_status, main_rank, bsr_rank, screen_size, count_of_reviews,
                   crawl_datetime
              FROM Walmart_tv_detail_crawled
             WHERE product_url = ANY(%s)
               AND crawl_datetime >= %s AND crawl_datetime <= %s
             ORDER BY product_url, crawl_datetime DESC
        """, (retail_missing, win_start_str, win_end_str))
        detail_by_url = {row[0]: row for row in cur.fetchall()}

        insertable = [u for u in retail_missing if u in detail_by_url]
        need_recrawl = [u for u in retail_missing if u not in detail_by_url]

        print(f"\n[SUMMARY]")
        print(f"   retail_missing total            : {len(retail_missing)}")
        print(f"   → INSERT-able (detail in window): {len(insertable)}")
        print(f"   → need recrawl (no detail)      : {len(need_recrawl)}")

        if need_recrawl:
            print(f"\n[NEED RECRAWL] (run wmart_tv_recover_main.py after this script)")
            for u in need_recrawl:
                print(f"     {u[:100]}")

        if not insertable:
            print("\n[OK] Nothing to INSERT.")
            return

        print(f"\n[INSERT CANDIDATES]")
        for u in insertable:
            r = detail_by_url[u]
            print(f"     main_rank={r[24]} bsr_rank={r[25]} detail_dt={r[28]} | {u[:70]}")

        resp = input(
            f"\nINSERT {len(insertable)} row(s) into tv_retail_com "
            f"(crawl_datetime={target_str})? [y/N] "
        )
        if resp.strip().lower() != 'y':
            print("[ABORT] No changes made")
            return

        inserted = 0
        for u in insertable:
            r = detail_by_url[u]
            cur.execute("""
                INSERT INTO tv_retail_com
                (item, account_name, page_type, count_of_reviews, retailer_sku_name, product_url,
                 star_rating, count_of_star_ratings, screen_size, sku_popularity,
                 final_sku_price, original_sku_price, savings, discount_type, offer,
                 pick_up_availability, shipping_availability, delivery_availability, shipping_info,
                 available_quantity_for_purchase, inventory_status, sku_status, retailer_membership_discounts,
                 detailed_review_content, summarized_review_content, top_mentions, recommendation_intent,
                 main_rank, bsr_rank, trend_rank, rank_1, rank_2, promotion_position,
                 number_of_ppl_purchased_yesterday, number_of_ppl_added_to_carts,
                 number_of_units_purchased_past_month, retailer_sku_name_similar,
                 estimated_annual_electricity_use, promotion_type, model_year,
                 calendar_week, crawl_datetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s)
            """, (
                r[3],                      # item
                'Walmart',                 # account_name
                r[1],                      # page_type
                _safe_int(r[27]),          # count_of_reviews (int)
                r[2],                      # retailer_sku_name
                r[0],                      # product_url
                r[4],                      # star_rating
                r[11],                     # count_of_star_ratings
                r[26],                     # screen_size
                r[7],                      # sku_popularity
                r[15],                     # final_sku_price
                r[16],                     # original_sku_price
                r[8],                      # savings
                r[9],                      # discount_type
                None,                      # offer
                r[17],                     # pick_up_availability
                r[18],                     # shipping_availability
                r[19],                     # delivery_availability
                r[10],                     # shipping_info
                r[22],                     # available_quantity_for_purchase
                r[23],                     # inventory_status
                r[20],                     # sku_status
                r[21],                     # retailer_membership_discounts
                r[13],                     # detailed_review_content
                None,                      # summarized_review_content
                None,                      # top_mentions
                None,                      # recommendation_intent
                r[24],                     # main_rank
                r[25],                     # bsr_rank
                None,                      # trend_rank
                None,                      # rank_1
                None,                      # rank_2
                None,                      # promotion_position
                r[5],                      # number_of_ppl_purchased_yesterday
                r[6],                      # number_of_ppl_added_to_carts
                None,                      # number_of_units_purchased_past_month
                r[12],                     # retailer_sku_name_similar
                None,                      # estimated_annual_electricity_use
                None,                      # promotion_type
                None,                      # model_year (NULL; post-fill if needed)
                r[14],                     # calendar_week
                target_str,                # crawl_datetime (session target)
            ))
            inserted += 1

        conn.commit()
        print(f"\n[OK] tv_retail_com inserted: {inserted} row(s)")

        if need_recrawl:
            print(f"\n[NEXT] Recrawl remaining {len(need_recrawl)} URL(s):")
            print(f"    python wmart_tv_recover_main.py {args.main1_batch_id} {args.main2_batch_id}")

    except Exception as e:
        conn.rollback()
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
