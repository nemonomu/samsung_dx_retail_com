"""
Recovery for missing BSR ranks in batch 20260423_010657.

누락 2개 (Amazon BSR 원본 수집 누락):
  rank 45 -> BSR page 1 (ranks 1-50) 스크랩으로 URL 획득
  rank 73 -> BSR page 2 (ranks 51-100) 스크랩으로 URL 획득

규칙:
  - crawl_datetime만 세션 범위(2026-04-22 12:09 ~ 14:04) 임의값
  - 나머지 모든 필드는 실수집 or 동일 URL의 기존 세션 데이터 JOIN
  - tv_retail_com 구(舊) row(batch_id=NULL)는 item+crawl_datetime+account_name으로 UPDATE 매칭

동작:
  1) amazon_tv_bsr batch=20260423_010657의 ranks 45, 73 존재 여부 preflight
  2) 필요한 BSR page 접속 (page 1, page 2) → 각 rank container에서 URL/name/reviews/rating 추출
  3) 사용자 확인 prompt
  4) amazon_tv_bsr INSERT (2건)
  5) 각 URL의 ASIN이 amazon_tv_detail_crawled batch=20260423_010917에 이미 있나 조회
     - HIT: detail_crawled + tv_retail_com UPDATE (JOIN, 브라우저 추가 접근 없음)
     - MISS: crawler.scrape_detail_page로 상세 스크랩 → 신규 INSERT → crawl_datetime 세션값 정규화

사용법:
  python recover_bsr_20260423_010657.py
"""
import sys
import os
import time
import random
import re
from datetime import datetime
import pytz
import psycopg2
from lxml import html

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DB_CONFIG
from amazon_tv_dt1 import AmazonDetailCrawler

TARGET_BSR_BATCH = '20260423_010657'
TARGET_DT_BATCH = '20260423_010917'
BSR_PAGE_URLS = {
    1: 'https://www.amazon.com/Best-Sellers-Electronics-Televisions/zgbs/electronics/172659/ref=zg_bs_nav_electronics_2_1266092011',
    2: 'https://www.amazon.com/Best-Sellers-Electronics-Televisions/zgbs/electronics/172659/ref=zg_bs_pg_2_electronics?_encoding=UTF8&pg=2',
}
CALENDAR_WEEK = 'w17'
SESSION_CRAWL_DT = '2026-04-22 13:00:00'  # 세션 범위 12:09:52 ~ 14:04:55 내

# (rank, page) 매핑
MISSING = [(45, 1), (73, 2)]


def load_bsr_xpaths(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT data_field, xpath FROM public.xpath_selectors
        WHERE mall_name='Amazon' AND page_type='bsr_page' AND is_active=TRUE
    """)
    xp = {r[0]: r[1] for r in cur.fetchall()}
    cur.close()
    return xp


def scrape_bsr_ranks(page, xpaths, page_url, target_ranks):
    """BSR page 접속 후 지정된 rank들의 container 추출.

    returns: dict {rank: {bsr_rank, product_name, product_url, count_of_reviews, star_rating}}
    """
    print(f"\n[STEP] Loading BSR page: {page_url[:80]}...")
    page.get(page_url)
    time.sleep(random.uniform(8, 12))

    for _ in range(6):
        page.scroll.to_bottom()
        time.sleep(random.uniform(1.5, 2.5))

    tree = html.fromstring(page.html)
    containers = tree.xpath(xpaths['base_container'])
    print(f"[INFO] Found {len(containers)} containers on page")

    found = {}
    for c in containers:
        rank_nodes = c.xpath(xpaths['rank'])
        if not rank_nodes:
            continue
        n = rank_nodes[0]
        rank_text = n.text_content().strip() if hasattr(n, 'text_content') else str(n).strip()
        try:
            rank = int(rank_text.replace('#', '').strip())
        except ValueError:
            continue
        if rank not in target_ranks:
            continue

        name_nodes = c.xpath(xpaths['product_name'])
        product_name = name_nodes[0].text_content().strip() if name_nodes else None

        url_nodes = c.xpath(xpaths['product_url'])
        product_url = None
        if url_nodes:
            raw = url_nodes[0]
            if isinstance(raw, str):
                product_url = raw if raw.startswith('http') else f'https://www.amazon.com{raw}'
            else:
                href = raw.get('href') if hasattr(raw, 'get') else None
                if href:
                    product_url = href if href.startswith('http') else f'https://www.amazon.com{href}'

        cor_xp = xpaths.get('count_of_reviews')
        cor_nodes = c.xpath(cor_xp) if cor_xp else []
        cor = cor_nodes[0].text_content().strip() if cor_nodes else None

        sr_xp = xpaths.get('star_rating')
        sr_nodes = c.xpath(sr_xp) if sr_xp else []
        sr_raw = sr_nodes[0].text_content().strip() if sr_nodes else None
        sr_match = re.search(r'[\d.]+', sr_raw or '')
        star_rating = sr_match.group() if sr_match else None

        found[rank] = {
            'bsr_rank': rank,
            'product_name': product_name,
            'product_url': product_url,
            'count_of_reviews': cor,
            'star_rating': star_rating,
        }
    return found


def insert_amazon_tv_bsr(conn, data):
    """amazon_tv_bsr INSERT — 세션 시간대 crawl_strdatetime (batch_id=20260423_010657)"""
    cur = conn.cursor()
    # 세션 시간대: batch_id='20260423_010657' → 2026-04-23 01:06:57 KST
    crawl_strdt = '202604230106' + str(random.randint(5800, 6000)) + str(random.randint(1000, 9999))
    cur.execute("""
        INSERT INTO public.amazon_tv_bsr
        (account_name, bsr_rank, page_type, Retailer_SKU_Name, product_url,
         final_sku_price, original_sku_price, count_of_reviews, star_rating,
         batch_id, calendar_week, crawl_strdatetime)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, ('Amazon', data['bsr_rank'], 'bsr', data['product_name'], data['product_url'],
          None, None, data['count_of_reviews'], data['star_rating'],
          TARGET_BSR_BATCH, CALENDAR_WEEK, crawl_strdt))
    new_id = cur.fetchone()[0]
    cur.close()
    return new_id


def handle_hit(conn, rank, asin, existing_row):
    """detail_crawled에 이미 있으면 UPDATE (JOIN 처리)"""
    dc_id, cur_bsr, cur_main, existing_dt = existing_row
    print(f"\n[HIT] rank={rank} item={asin}: detail id={dc_id}, bsr={cur_bsr}, main={cur_main}, dt={existing_dt}")

    if cur_bsr is not None:
        print(f"[SKIP] bsr_rank already set to {cur_bsr}")
        return False

    cur = conn.cursor()
    cur.execute("""
        UPDATE public.amazon_tv_detail_crawled SET bsr_rank=%s WHERE id=%s
    """, (rank, dc_id))
    d_rows = cur.rowcount
    cur.execute("""
        UPDATE public.tv_retail_com SET bsr_rank=%s
        WHERE item=%s AND crawl_datetime=%s AND account_name='Amazon' AND bsr_rank IS NULL
    """, (rank, asin, existing_dt))
    t_rows = cur.rowcount
    cur.close()
    print(f"      -> detail={d_rows}, retail={t_rows}")
    return True


def handle_miss(conn, crawler, bsr_data, rank, asin):
    """detail_crawled에 없으면 상세 스크랩 후 crawl_datetime 정규화"""
    print(f"\n[MISS] rank={rank} item={asin}: fresh scraping")

    pcur = conn.cursor()
    pcur.execute("""
        SELECT DISTINCT item FROM public.amazon_tv_detail_crawled
        WHERE batch_id=%s AND item IS NOT NULL
    """, (TARGET_DT_BATCH,))
    crawler.processed_asins = set()
    for r in pcur.fetchall():
        crawler.processed_asins.add(r[0])
    pcur.close()

    pcur = conn.cursor()
    pcur.execute("SELECT COUNT(*) FROM public.amazon_tv_detail_crawled WHERE batch_id=%s",
                 (TARGET_DT_BATCH,))
    pre_count = pcur.fetchone()[0]
    pcur.close()

    url_data = {
        'asin': asin,
        'page_type': 'bsr',
        'url': bsr_data['product_url'],
        'main_rank': None,
        'bsr_rank': rank,
    }
    success = crawler.scrape_detail_page(url_data)
    print(f"      scrape_detail_page -> {success}")

    pcur = conn.cursor()
    pcur.execute("SELECT COUNT(*) FROM public.amazon_tv_detail_crawled WHERE batch_id=%s",
                 (TARGET_DT_BATCH,))
    post_count = pcur.fetchone()[0]
    pcur.close()

    if post_count > pre_count:
        ucur = conn.cursor()
        ucur.execute("""
            UPDATE public.amazon_tv_detail_crawled
            SET crawl_datetime=%s, calendar_week=%s
            WHERE batch_id=%s AND bsr_rank=%s
        """, (SESSION_CRAWL_DT, CALENDAR_WEEK, TARGET_DT_BATCH, rank))
        d_rows = ucur.rowcount
        ucur.execute("""
            UPDATE public.tv_retail_com
            SET crawl_datetime=%s, calendar_week=%s
            WHERE batch_id=%s AND bsr_rank=%s
        """, (SESSION_CRAWL_DT, CALENDAR_WEEK, TARGET_DT_BATCH, rank))
        t_rows = ucur.rowcount
        ucur.close()
        print(f"      NEW row + crawl_datetime normalized: detail={d_rows}, retail={t_rows}")
    else:
        print(f"      DEDUP path taken (existing row updated by dt1 patch)")


def main():
    print(f"=== BSR Recovery for batch {TARGET_BSR_BATCH} ===")
    print(f"    Target DT batch: {TARGET_DT_BATCH}")
    print(f"    Missing ranks: {[r for r,_ in MISSING]}\n")

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    # 사전 상태
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(DISTINCT bsr_rank) FROM public.amazon_tv_detail_crawled
        WHERE batch_id=%s AND bsr_rank IS NOT NULL
    """, (TARGET_DT_BATCH,))
    pre_detail = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM public.amazon_tv_bsr WHERE batch_id=%s",
                (TARGET_BSR_BATCH,))
    pre_bsr = cur.fetchone()[0]
    print(f"[PRE] detail_crawled distinct bsr_rank: {pre_detail}")
    print(f"[PRE] amazon_tv_bsr rows: {pre_bsr}")

    # Preflight: 이미 있으면 제외
    ranks_to_process = []
    for rank, page_num in MISSING:
        cur.execute("SELECT id FROM public.amazon_tv_bsr WHERE batch_id=%s AND bsr_rank=%s",
                    (TARGET_BSR_BATCH, rank))
        if cur.fetchone():
            print(f"[SKIP] amazon_tv_bsr already has rank {rank}")
        else:
            ranks_to_process.append((rank, page_num))
    cur.close()

    if not ranks_to_process:
        print("\n[DONE] 처리할 rank 없음 (모두 이미 존재)")
        conn.close()
        return

    bsr_xpaths = load_bsr_xpaths(conn)
    print(f"[OK] Loaded {len(bsr_xpaths)} BSR xpaths")

    crawler = AmazonDetailCrawler()
    crawler.batch_id = TARGET_DT_BATCH
    if not crawler.connect_db():
        return
    if not crawler.load_xpaths():
        return
    crawler.setup_driver()

    try:
        # 페이지별로 그룹화
        by_page = {}
        for rank, page_num in ranks_to_process:
            by_page.setdefault(page_num, []).append(rank)

        # 각 페이지 1회 스크랩
        scraped = {}
        for page_num, ranks_in_page in by_page.items():
            page_url = BSR_PAGE_URLS.get(page_num)
            if not page_url:
                print(f"[ERROR] no URL configured for page {page_num}")
                continue
            found = scrape_bsr_ranks(crawler.page, bsr_xpaths, page_url, ranks_in_page)
            for rank, data in found.items():
                scraped[rank] = data

        # 결과 프린트
        missing_after_scrape = []
        for rank, page_num in ranks_to_process:
            if rank not in scraped:
                missing_after_scrape.append((rank, page_num))
                continue
            d = scraped[rank]
            print(f"\n[FOUND] rank {rank} (page {page_num}):")
            print(f"  name : {(d['product_name'] or '')[:80]}")
            print(f"  url  : {d['product_url'][:100] if d['product_url'] else None}")
            print(f"  star : {d['star_rating']}   reviews: {d['count_of_reviews']}")

        if missing_after_scrape:
            print(f"\n[WARNING] BSR page에서 못 찾은 rank: {[r for r,_ in missing_after_scrape]}")

        if not scraped:
            print("\n[ERROR] 스크랩 실패, 종료")
            return

        resp = input(f"\n위 {len(scraped)}개 rank을 INSERT + detail 처리 진행? [y/N]: ").strip().lower()
        if resp != 'y':
            print("[CANCEL] 사용자 취소")
            return

        # INSERT amazon_tv_bsr
        for rank, data in scraped.items():
            new_id = insert_amazon_tv_bsr(conn, data)
            print(f"[OK] amazon_tv_bsr INSERT rank={rank} id={new_id}")

        # detail HIT/MISS 처리
        print(f"\n--- Phase: detail_crawled / tv_retail_com 처리 ---")
        for rank, data in scraped.items():
            print(f"\n>>> rank {rank}")
            asin = crawler.extract_asin(data['product_url'])
            if not asin or len(asin) != 10:
                print(f"  [ERROR] invalid ASIN from url")
                continue

            dcur = conn.cursor()
            dcur.execute("""
                SELECT id, bsr_rank, main_rank, crawl_datetime
                FROM public.amazon_tv_detail_crawled
                WHERE batch_id=%s AND item=%s
            """, (TARGET_DT_BATCH, asin))
            existing = dcur.fetchone()
            dcur.close()

            if existing:
                handle_hit(conn, rank, asin, existing)
            else:
                handle_miss(conn, crawler, data, rank, asin)

    finally:
        try:
            crawler.page.quit()
        except Exception:
            pass
        try:
            crawler.db_conn.close()
        except Exception:
            pass

    # Post verification
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(DISTINCT bsr_rank) FROM public.amazon_tv_detail_crawled
        WHERE batch_id=%s AND bsr_rank IS NOT NULL
    """, (TARGET_DT_BATCH,))
    post_detail = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM public.amazon_tv_bsr WHERE batch_id=%s",
                (TARGET_BSR_BATCH,))
    post_bsr = cur.fetchone()[0]
    print(f"\n[POST] detail_crawled distinct bsr_rank: {pre_detail} -> {post_detail}")
    print(f"[POST] amazon_tv_bsr rows: {pre_bsr} -> {post_bsr}")

    cur.execute("""
        SELECT generate_series AS missing FROM generate_series(1, 100)
        WHERE generate_series NOT IN (
          SELECT bsr_rank FROM public.amazon_tv_detail_crawled
          WHERE batch_id=%s AND bsr_rank IS NOT NULL
        )
    """, (TARGET_DT_BATCH,))
    still_missing = [r[0] for r in cur.fetchall()]
    print(f"[POST] detail_crawled still missing (1-100): {still_missing}")
    cur.close()

    conn.close()


if __name__ == '__main__':
    main()
